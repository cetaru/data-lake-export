/**
 * Curated schedule-risk datasets, computed with DuckDB over the snapshot that
 * was just exported for a tenant (no extra reads from PocketBase):
 *
 *   delay_stats      how late revisions have historically been issued, by
 *                    specialty / document type / revision
 *   risk_revisions   every pending revision with its projected issue date
 *   risk_daily       one row per project: projected finish, slip, hours
 *                    outlook and a 0-100 risk score with its reasons
 *
 * Method (empirical, no model training — every number is traceable):
 * 1. A revision (A, B, 0) of a non-cancelled document is "issued" when
 *    list_document_forecast.erev_* is set (cumulative credit reached its
 *    threshold) and "pending" otherwise.
 * 2. delay = issue date − reference date, in days, measured two ways:
 *    against the planned date (basis 'planned') and against the forecast date
 *    (basis 'forecast', i.e. how wrong forecasts usually are).
 * 3. For each pending revision the reference date is its forecast date, or
 *    the planned date when there is no forecast. The projected issue date is
 *    reference + typical delay of similar revisions (median → p50, 80th
 *    percentile → p80), never earlier than the snapshot date. "Similar" is the
 *    most specific group with at least MIN_SAMPLE issued revisions:
 *    specialty + document type + revision → specialty + revision → revision.
 *    A revision that is ALREADY overdue by d days is projected from the
 *    snapshot date instead: plus how much longer past revisions that were
 *    more than d days late still took (delay − d). If nothing in history was
 *    ever that late, the reference date is probably out of date: the revision
 *    is flagged beyond_history and gets the usual delay of late revisions.
 * 4. Per project: projected finish = latest projected issue date; slip =
 *    projected finish − project end date. Hours outlook = logged + remaining
 *    vs. budget, and remaining hours ÷ hours logged per week over the last 4
 *    weeks. The score combines slip, SPI, overdue share and hours overrun.
 */
const MIN_SAMPLE = 15;
const REQUIRED = ['list_document_forecast', 'list_hours', 'projects', 'origen_type', 'specialties', 'document_type'];

// Dates arrive as text ('YYYY-MM-DD HH:MM:SS.sssZ' or '') or already typed, depending on the dataset.
const day = (col) => `TRY_CAST(substr(CAST(${col} AS VARCHAR), 1, 10) AS DATE)`;

/**
 * @param {object} opts
 * @param {import('@duckdb/node-api').DuckDBConnection} opts.con
 * @param {(name: string) => string|null} opts.rawParquet   path of an exported raw dataset, or null if missing
 * @param {string|null} opts.evmDailyParquet                path of this snapshot's curated evm_daily
 * @param {string} opts.snapshotDate                         YYYY-MM-DD
 * @param {(layer: string, name: string, selectSql: string) => Promise<void>} opts.copyToParquet
 * @returns {Promise<string[]>} warnings
 */
export async function exportRisk({ con, rawParquet, evmDailyParquet, snapshotDate, copyToParquet }) {
  const missing = REQUIRED.filter((name) => !rawParquet(name));
  if (missing.length || !evmDailyParquet) {
    return [`riesgo no calculado — faltan datasets: ${[...missing, ...(evmDailyParquet ? [] : ['evm_daily'])].join(', ')}`];
  }
  for (const name of REQUIRED) {
    await con.run(`CREATE OR REPLACE TEMP VIEW rk_${name} AS SELECT * FROM read_parquet('${rawParquet(name)}')`);
  }
  await con.run(`CREATE OR REPLACE TEMP VIEW rk_evm AS SELECT * FROM read_parquet('${evmDailyParquet}')`);
  const today = `DATE '${snapshotDate}'`;

  // One row per document revision that has a planned or forecast date (cancelled documents excluded).
  await con.run(`
    CREATE OR REPLACE TEMP TABLE rk_rev AS
    SELECT * FROM (
      SELECT f.id AS document_id, f.code AS document_code, f.name AS document_name, f.project AS project_id,
             f.specialty AS specialty_id, f.document_type AS document_type_id, r.rev AS revision,
             f.weight * CASE r.rev WHEN 'A' THEN e.pct_a WHEN 'B' THEN e.pct_b ELSE e.pct_0 END * 100 AS weight_pct,
             CASE r.rev WHEN 'A' THEN ${day('f.rev_a')}  WHEN 'B' THEN ${day('f.rev_b')}  ELSE ${day('f.rev_0')}  END AS planned_date,
             CASE r.rev WHEN 'A' THEN ${day('f.frev_a')} WHEN 'B' THEN ${day('f.frev_b')} ELSE ${day('f.frev_0')} END AS forecast_date,
             CASE r.rev WHEN 'A' THEN ${day('f.erev_a')} WHEN 'B' THEN ${day('f.erev_b')} ELSE ${day('f.erev_0')} END AS issued_date
      FROM rk_list_document_forecast f
      CROSS JOIN (VALUES ('A'), ('B'), ('0')) r(rev)
      LEFT JOIN rk_evm e ON e.project_id = f.project
      WHERE NOT EXISTS (SELECT 1 FROM rk_origen_type o WHERE o.id = f.origen_type AND o.ignored_count)
    ) WHERE planned_date IS NOT NULL OR forecast_date IS NOT NULL`);

  // Historical delays, at three levels of detail and on both bases.
  const statsLevel = (level, basis, groupCols) => {
    const ref = basis === 'planned' ? 'planned_date' : 'forecast_date';
    const col = (name) => (groupCols.includes(name) ? name : `CAST(NULL AS VARCHAR) AS ${name}`);
    return `
      SELECT '${basis}' AS basis, '${level}' AS level, ${col('specialty_id')}, ${col('document_type_id')}, revision,
             count(*) AS sample_size,
             round(100.0 * count(*) FILTER (issued_date <= ${ref}) / count(*), 1) AS on_time_pct,
             round(avg(issued_date - ${ref}), 1) AS mean_days,
             round(quantile_cont(issued_date - ${ref}, 0.5), 1) AS median_days,
             round(quantile_cont(issued_date - ${ref}, 0.8), 1) AS p80_days
      FROM rk_rev WHERE issued_date IS NOT NULL AND ${ref} IS NOT NULL
      GROUP BY ${[...groupCols, 'revision'].join(', ')}`;
  };
  const levels = [
    ['specialty_type_revision', ['specialty_id', 'document_type_id']],
    ['specialty_revision', ['specialty_id']],
    ['revision', []],
  ];
  await con.run(`
    CREATE OR REPLACE TEMP TABLE rk_stats AS
    ${levels.flatMap(([level, cols]) => ['planned', 'forecast'].map((basis) => statsLevel(level, basis, cols))).join('\n UNION ALL ')}`);

  await copyToParquet('curated', 'delay_stats', `
    SELECT s.basis, s.level, s.specialty_id, sp.name AS specialty, s.document_type_id, dt.name AS document_type,
           s.revision, s.sample_size, s.on_time_pct, s.mean_days, s.median_days, s.p80_days
    FROM rk_stats s
    LEFT JOIN rk_specialties sp ON sp.id = s.specialty_id
    LEFT JOIN rk_document_type dt ON dt.id = s.document_type_id`);

  // Individual historical delays, to project revisions that are already overdue.
  await con.run(`
    CREATE OR REPLACE TEMP TABLE rk_hist AS
    SELECT 'planned' AS basis, revision, issued_date - planned_date AS delay_days
    FROM rk_rev WHERE issued_date IS NOT NULL AND planned_date IS NOT NULL
    UNION ALL
    SELECT 'forecast', revision, issued_date - forecast_date
    FROM rk_rev WHERE issued_date IS NOT NULL AND forecast_date IS NOT NULL`);
  await con.run(`
    CREATE OR REPLACE TEMP TABLE rk_late AS
    SELECT basis, revision, quantile_cont(delay_days, 0.5) AS late_p50, quantile_cont(delay_days, 0.8) AS late_p80
    FROM rk_hist WHERE delay_days > 0 GROUP BY 1, 2`);

  // Pending revisions with their projected issue dates.
  const pick = (alias, level) => `
    LEFT JOIN rk_stats ${alias} ON ${alias}.level = '${level}' AND ${alias}.basis = p.basis AND ${alias}.revision = p.revision
      AND ${alias}.sample_size >= ${MIN_SAMPLE}
      ${level !== 'revision' ? `AND ${alias}.specialty_id IS NOT DISTINCT FROM p.specialty_id` : ''}
      ${level === 'specialty_type_revision' ? `AND ${alias}.document_type_id IS NOT DISTINCT FROM p.document_type_id` : ''}`;
  await con.run(`
    CREATE OR REPLACE TEMP TABLE rk_pending AS
    WITH p AS (
      SELECT *, coalesce(forecast_date, planned_date) AS reference_date,
             CASE WHEN forecast_date IS NOT NULL THEN 'forecast' ELSE 'planned' END AS basis
      FROM rk_rev WHERE issued_date IS NULL
    ), q AS (
      SELECT p.*,
             greatest(${today} - p.reference_date, 0) AS days_overdue,
             coalesce(s1.level, s2.level, s3.level) AS stats_level,
             coalesce(s1.sample_size, s2.sample_size, s3.sample_size) AS stats_sample_size,
             greatest(coalesce(s1.median_days, s2.median_days, s3.median_days, 0), 0) AS typical_delay_days,
             greatest(coalesce(s1.p80_days, s2.p80_days, s3.p80_days, 0), 0) AS pessimistic_delay_days
      FROM p ${pick('s1', 'specialty_type_revision')} ${pick('s2', 'specialty_revision')} ${pick('s3', 'revision')}
    ), r AS (
      -- already overdue by d days: how much longer did revisions that were more than d days late take?
      SELECT q.*,
             (SELECT count(*) FROM rk_hist h WHERE h.basis = q.basis AND h.revision = q.revision AND h.delay_days > q.days_overdue) AS later_sample,
             (SELECT quantile_cont(h.delay_days - q.days_overdue, 0.5) FROM rk_hist h WHERE h.basis = q.basis AND h.revision = q.revision AND h.delay_days > q.days_overdue) AS later_p50,
             (SELECT quantile_cont(h.delay_days - q.days_overdue, 0.8) FROM rk_hist h WHERE h.basis = q.basis AND h.revision = q.revision AND h.delay_days > q.days_overdue) AS later_p80
      FROM q
    ), t AS (
      SELECT r.*, r.reference_date < ${today} AS overdue,
             r.reference_date < ${today} AND r.later_sample < 5 AS beyond_history,
             CASE WHEN r.reference_date >= ${today} THEN NULL
                  WHEN r.later_sample >= 5 THEN r.later_p50 ELSE coalesce(l.late_p50, 0) END AS remaining_days_p50,
             CASE WHEN r.reference_date >= ${today} THEN NULL
                  WHEN r.later_sample >= 5 THEN r.later_p80 ELSE coalesce(l.late_p80, 0) END AS remaining_days_p80
      FROM r LEFT JOIN rk_late l ON l.basis = r.basis AND l.revision = r.revision
    )
    SELECT document_id, document_code, document_name, project_id, specialty_id, document_type_id, revision,
           round(weight_pct, 4) AS weight_pct, planned_date, forecast_date, reference_date, basis AS reference_basis,
           overdue, days_overdue, beyond_history,
           stats_level, stats_sample_size, typical_delay_days, pessimistic_delay_days,
           CASE WHEN overdue THEN ${today} + CAST(ceil(remaining_days_p50) AS INTEGER)
                ELSE greatest(reference_date + CAST(round(typical_delay_days) AS INTEGER), ${today}) END AS projected_date_p50,
           CASE WHEN overdue THEN ${today} + CAST(ceil(remaining_days_p80) AS INTEGER)
                ELSE greatest(reference_date + CAST(round(pessimistic_delay_days) AS INTEGER), ${today}) END AS projected_date_p80
    FROM t`);

  await copyToParquet('curated', 'risk_revisions', `
    SELECT p.*, pr.code AS project_code, sp.name AS specialty, dt.name AS document_type
    FROM rk_pending p
    LEFT JOIN rk_projects pr ON pr.id = p.project_id
    LEFT JOIN rk_specialties sp ON sp.id = p.specialty_id
    LEFT JOIN rk_document_type dt ON dt.id = p.document_type_id`);

  // Per project.
  await copyToParquet('curated', 'risk_daily', `
    WITH pend AS (
      SELECT project_id, count(*) AS pending_revisions,
             count(*) FILTER (overdue) AS overdue_revisions,
             count(*) FILTER (beyond_history) AS stale_revisions,
             coalesce(sum(weight_pct), 0) AS pending_weight_pct,
             coalesce(sum(weight_pct) FILTER (overdue), 0) AS overdue_weight_pct,
             max(projected_date_p50) AS projected_finish_p50,
             max(projected_date_p80) AS projected_finish_p80
      FROM rk_pending GROUP BY 1
    ), docs AS (
      SELECT project AS project_id, sum(remaining_hs) AS remaining_hours
      FROM rk_list_document_forecast f
      WHERE NOT EXISTS (SELECT 1 FROM rk_origen_type o WHERE o.id = f.origen_type AND o.ignored_count)
      GROUP BY 1
    ), recent AS (
      SELECT project AS project_id, sum(hours) / 4.0 AS weekly_hours
      FROM rk_list_hours
      WHERE ${day('date')} > ${today} - 28 AND ${day('date')} <= ${today}
      GROUP BY 1
    ), base AS (
      SELECT pr.id AS project_id, pr.code AS project_code, pr.name AS project_name, pr.status,
             ${day('pr.start_date')} AS start_date, ${day('pr.end_date')} AS end_date,
             coalesce(pend.pending_revisions, 0) AS pending_revisions,
             coalesce(pend.overdue_revisions, 0) AS overdue_revisions,
             coalesce(pend.stale_revisions, 0) AS stale_revisions,
             round(coalesce(pend.pending_weight_pct, 0), 2) AS pending_weight_pct,
             round(coalesce(pend.overdue_weight_pct, 0), 2) AS overdue_weight_pct,
             pend.projected_finish_p50, pend.projected_finish_p80,
             round(e.pv_pct, 2) AS pv_pct, round(e.ev_pct, 2) AS ev_pct, round(e.spi, 3) AS spi, round(e.hpi, 3) AS hpi,
             round(e.hours_oc, 1) AS hours_budget, round(e.hours_actual, 1) AS hours_logged,
             round(coalesce(docs.remaining_hours, 0), 1) AS remaining_hours,
             round(coalesce(recent.weekly_hours, 0), 1) AS weekly_hours_last_4w
      FROM rk_projects pr
      LEFT JOIN pend ON pend.project_id = pr.id
      LEFT JOIN docs ON docs.project_id = pr.id
      LEFT JOIN recent ON recent.project_id = pr.id
      LEFT JOIN rk_evm e ON e.project_id = pr.id
    ), calc AS (
      SELECT *,
             projected_finish_p50 - end_date AS slip_days_p50,
             projected_finish_p80 - end_date AS slip_days_p80,
             round(hours_logged + remaining_hours, 1) AS hours_at_completion,
             CASE WHEN hours_budget > 0 THEN round(100.0 * ((hours_logged + remaining_hours) / hours_budget - 1), 1) END AS hours_overrun_pct,
             CASE WHEN remaining_hours > 0 AND weekly_hours_last_4w > 0
                  THEN ${today} + CAST(ceil(7.0 * remaining_hours / weekly_hours_last_4w) AS INTEGER) END AS capacity_finish,
             -- share of the pending work that is overdue: by progress weight, or by count when documents carry no weight
             CASE WHEN pending_weight_pct > 0 THEN round(100.0 * overdue_weight_pct / pending_weight_pct, 1)
                  WHEN pending_revisions > 0 THEN round(100.0 * overdue_revisions / pending_revisions, 1)
                  ELSE 0 END AS overdue_share_pct
      FROM base
    ), scored AS (
      SELECT *,
             CASE WHEN pending_revisions = 0 THEN 0 ELSE
               40 * least(greatest(coalesce(slip_days_p80, 0), 0) / 60.0, 1) END AS score_schedule,
             CASE WHEN pending_revisions = 0 OR coalesce(pv_pct, 0) < 5 OR coalesce(spi, 0) = 0 THEN 0 ELSE
               25 * least(greatest(1 - spi, 0) / 0.3, 1) END AS score_progress,
             CASE WHEN pending_revisions = 0 THEN 0 ELSE 15 * overdue_share_pct / 100.0 END AS score_overdue,
             CASE WHEN pending_revisions = 0 THEN 0 ELSE
               20 * least(greatest(coalesce(hours_overrun_pct, 0), 0) / 30.0, 1) END AS score_hours
      FROM calc
    )
    SELECT project_id, project_code, project_name, status, start_date, end_date,
           pending_revisions, overdue_revisions, stale_revisions, pending_weight_pct, overdue_weight_pct, overdue_share_pct,
           projected_finish_p50, projected_finish_p80, slip_days_p50, slip_days_p80,
           pv_pct, ev_pct, spi, hpi,
           hours_budget, hours_logged, remaining_hours, hours_at_completion, hours_overrun_pct,
           weekly_hours_last_4w, capacity_finish, capacity_finish - end_date AS capacity_slip_days,
           CAST(round(score_schedule + score_progress + score_overdue + score_hours) AS INTEGER) AS risk_score,
           CASE WHEN pending_revisions = 0 THEN 'none'
                WHEN score_schedule + score_progress + score_overdue + score_hours >= 60 THEN 'high'
                WHEN score_schedule + score_progress + score_overdue + score_hours >= 30 THEN 'medium'
                ELSE 'low' END AS risk_level,
           round(score_schedule, 1) AS score_schedule, round(score_progress, 1) AS score_progress,
           round(score_overdue, 1) AS score_overdue, round(score_hours, 1) AS score_hours,
           concat_ws('; ',
             CASE WHEN pending_revisions > 0 AND end_date IS NULL THEN 'project has no end date: slip cannot be measured' END,
             CASE WHEN slip_days_p80 > 0 THEN 'projected finish ' || CAST(projected_finish_p80 AS VARCHAR) || ' is ' || slip_days_p80 || ' days after the end date (pessimistic); ' || greatest(slip_days_p50, 0) || ' days in the typical case' END,
             CASE WHEN score_progress > 0 THEN 'SPI ' || spi || ': earned ' || ev_pct || '% vs planned ' || pv_pct || '%' END,
             CASE WHEN overdue_revisions > 0 THEN overdue_revisions || ' of ' || pending_revisions || ' pending revisions are overdue (' || overdue_share_pct || '% of the pending work)' END,
             CASE WHEN stale_revisions > 0 THEN stale_revisions || ' pending revisions are overdue longer than any past revision: their dates look out of date' END,
             CASE WHEN hours_overrun_pct > 0 THEN 'hours at completion ' || hours_at_completion || ' vs budget ' || hours_budget || ' (+' || hours_overrun_pct || '%)' END,
             CASE WHEN capacity_finish > end_date THEN 'at the last 4 weeks'' pace (' || weekly_hours_last_4w || ' h/week) remaining hours finish on ' || CAST(capacity_finish AS VARCHAR) END,
             CASE WHEN pending_revisions > 0 AND remaining_hours > 0 AND weekly_hours_last_4w = 0 THEN 'no hours logged in the last 4 weeks while ' || remaining_hours || ' h remain' END
           ) AS risk_reasons
    FROM scored`);

  return [];
}
