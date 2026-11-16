#!/usr/bin/env node
/**
 * Backtest of the schedule-risk projection (src/risk.mjs) for ONE tenant.
 *
 * Question: had we run the projection in the past, how close would it have
 * been to what actually happened?
 *
 * Method — for every FINISHED project (status inactive, nothing pending,
 * start and end dates set) we go back to three moments T of its planned
 * duration (25 %, 50 %, 75 %) and rebuild what was known then:
 *   - a revision was pending at T if its document already existed and it was
 *     issued after T;
 *   - the delay statistics use only revisions (of any project) issued by T.
 * Then we project each pending revision exactly like src/risk.mjs does and
 * compare with the real issue dates.
 *
 * Deliberate limits (stated in the output):
 *   - Reference = PLANNED date only. Forecast dates are overwritten on every
 *     update, so the forecast known at T cannot be recovered; using today's
 *     forecast would leak the future. Production also uses forecasts, so it
 *     should do at least as well as this test.
 *   - Planned dates and project end dates are today's values; a re-baseline
 *     after T makes the past look more predictable than it was.
 *   - Only the date projection is tested, not the SPI / hours parts of the
 *     risk score (they need snapshots the lake did not have yet).
 *
 * Usage: LAKE_DIR=... TENANT_ID=tsp node bin/backtest.mjs
 * Reads the tenant's latest lake snapshot. Prints aggregates only.
 */
import { DuckDBInstance } from '@duckdb/node-api';
import { readdirSync, existsSync } from 'node:fs';
import { join } from 'node:path';

const LAKE_DIR = process.env.LAKE_DIR || '/data/lake';
const TENANT = process.env.TENANT_ID;
const MIN_SAMPLE = 15;
const FRACTIONS = [0.25, 0.5, 0.75];
const LATE_TOLERANCE_DAYS = 7; // a project counts as "late" beyond this

if (!TENANT) { console.error('Uso: LAKE_DIR=<lake> TENANT_ID=<code> node bin/backtest.mjs'); process.exit(1); }

function latest(dataset) {
  const dir = join(LAKE_DIR, 'raw', dataset, `tenant_id=${TENANT}`);
  if (!existsSync(dir)) throw new Error(`sin datos de ${dataset} para ${TENANT} en ${LAKE_DIR}`);
  const snap = readdirSync(dir).filter((d) => d.startsWith('snapshot_date=')).sort().at(-1);
  return join(dir, snap, 'data.parquet');
}

const con = await (await DuckDBInstance.create(':memory:')).connect();
for (const name of ['list_document_forecast', 'documents', 'projects', 'origen_type']) {
  await con.run(`CREATE VIEW ${name} AS SELECT * FROM read_parquet('${latest(name)}')`);
}
const day = (col) => `TRY_CAST(substr(CAST(${col} AS VARCHAR), 1, 10) AS DATE)`;
const rows = async (sql) => (await con.runAndReadAll(sql)).getRowObjectsJson();

// One row per revision with a planned date (cancelled documents excluded).
await con.run(`
  CREATE TABLE rev AS
  SELECT * FROM (
    SELECT f.id AS document_id, f.project AS project_id, f.specialty AS specialty_id, f.document_type AS document_type_id,
           r.rev AS revision, ${day('d.created')} AS created_date,
           CASE r.rev WHEN 'A' THEN ${day('f.rev_a')}  WHEN 'B' THEN ${day('f.rev_b')}  ELSE ${day('f.rev_0')}  END AS planned_date,
           CASE r.rev WHEN 'A' THEN ${day('f.erev_a')} WHEN 'B' THEN ${day('f.erev_b')} ELSE ${day('f.erev_0')} END AS issued_date
    FROM list_document_forecast f
    CROSS JOIN (VALUES ('A'), ('B'), ('0')) r(rev)
    LEFT JOIN documents d ON d.id = f.id
    WHERE NOT EXISTS (SELECT 1 FROM origen_type o WHERE o.id = f.origen_type AND o.ignored_count)
  ) WHERE planned_date IS NOT NULL`);

// Finished projects and their checkpoints.
await con.run(`
  CREATE TABLE proj AS
  SELECT p.id AS project_id, ${day('p.start_date')} AS start_date,
         coalesce(${day('p.end_date_original')}, ${day('p.end_date')}) AS end_date,
         x.actual_finish
  FROM projects p
  JOIN (SELECT project_id, max(issued_date) AS actual_finish, count(*) FILTER (issued_date IS NULL) AS pending
        FROM rev GROUP BY 1) x ON x.project_id = p.id
  WHERE p.status = 'inactive' AND x.pending = 0 AND x.actual_finish IS NOT NULL`);
await con.run(`DELETE FROM proj WHERE start_date IS NULL OR end_date IS NULL OR end_date <= start_date`);
await con.run(`
  CREATE TABLE cp AS
  SELECT p.*, f.frac, p.start_date + CAST(round(f.frac * (p.end_date - p.start_date)) AS INTEGER) AS t
  FROM proj p CROSS JOIN (VALUES ${FRACTIONS.map((f) => `(${f})`).join(', ')}) f(frac)`);

// Revisions pending at each checkpoint.
await con.run(`
  CREATE TABLE pend AS
  SELECT cp.project_id, cp.frac, cp.t, cp.end_date, cp.actual_finish,
         r.document_id, r.revision, r.specialty_id, r.document_type_id, r.planned_date, r.issued_date,
         greatest(cp.t - r.planned_date, 0) AS days_overdue
  FROM cp JOIN rev r ON r.project_id = cp.project_id
  WHERE r.issued_date > cp.t AND (r.created_date IS NULL OR r.created_date <= cp.t)`);

// Delay history known at each checkpoint date.
await con.run(`CREATE TABLE hist AS SELECT specialty_id, document_type_id, revision, issued_date, issued_date - planned_date AS delay_days FROM rev WHERE issued_date IS NOT NULL`);
await con.run(`CREATE TABLE tdates AS SELECT DISTINCT t FROM cp`);
const statsAt = (name, cols) => con.run(`
  CREATE TABLE ${name} AS
  SELECT d.t, ${cols.map((c) => `h.${c}`).join(', ')}, count(*) AS n,
         quantile_cont(h.delay_days, 0.5) AS med, quantile_cont(h.delay_days, 0.8) AS p80
  FROM tdates d JOIN hist h ON h.issued_date <= d.t
  GROUP BY ${['d.t', ...cols.map((c) => `h.${c}`)].join(', ')}`);
await statsAt('st1', ['specialty_id', 'document_type_id', 'revision']);
await statsAt('st2', ['specialty_id', 'revision']);
await statsAt('st3', ['revision']);
await con.run(`
  CREATE TABLE late AS
  SELECT d.t, h.revision, quantile_cont(h.delay_days, 0.5) AS late_p50, quantile_cont(h.delay_days, 0.8) AS late_p80
  FROM tdates d JOIN hist h ON h.issued_date <= d.t AND h.delay_days > 0 GROUP BY 1, 2`);

// Projection, same rules as src/risk.mjs.
await con.run(`
  CREATE TABLE proj_rev AS
  WITH q AS (
    SELECT p.*,
           greatest(coalesce(CASE WHEN s1.n >= ${MIN_SAMPLE} THEN s1.med END, CASE WHEN s2.n >= ${MIN_SAMPLE} THEN s2.med END, CASE WHEN s3.n >= ${MIN_SAMPLE} THEN s3.med END, 0), 0) AS typical,
           greatest(coalesce(CASE WHEN s1.n >= ${MIN_SAMPLE} THEN s1.p80 END, CASE WHEN s2.n >= ${MIN_SAMPLE} THEN s2.p80 END, CASE WHEN s3.n >= ${MIN_SAMPLE} THEN s3.p80 END, 0), 0) AS pessimistic,
           coalesce(s1.n >= ${MIN_SAMPLE}, false) OR coalesce(s2.n >= ${MIN_SAMPLE}, false) OR coalesce(s3.n >= ${MIN_SAMPLE}, false) AS has_history
    FROM pend p
    LEFT JOIN st1 s1 ON s1.t = p.t AND s1.revision = p.revision AND s1.specialty_id IS NOT DISTINCT FROM p.specialty_id AND s1.document_type_id IS NOT DISTINCT FROM p.document_type_id
    LEFT JOIN st2 s2 ON s2.t = p.t AND s2.revision = p.revision AND s2.specialty_id IS NOT DISTINCT FROM p.specialty_id
    LEFT JOIN st3 s3 ON s3.t = p.t AND s3.revision = p.revision
  ), r AS (
    SELECT q.*,
           (SELECT count(*) FROM hist h WHERE h.issued_date <= q.t AND h.revision = q.revision AND h.delay_days > q.days_overdue) AS later_n,
           (SELECT quantile_cont(h.delay_days - q.days_overdue, 0.5) FROM hist h WHERE h.issued_date <= q.t AND h.revision = q.revision AND h.delay_days > q.days_overdue) AS later_p50,
           (SELECT quantile_cont(h.delay_days - q.days_overdue, 0.8) FROM hist h WHERE h.issued_date <= q.t AND h.revision = q.revision AND h.delay_days > q.days_overdue) AS later_p80
    FROM q
  )
  SELECT r.*, greatest(r.planned_date, r.t) AS naive_date,
         CASE WHEN r.planned_date < r.t
              THEN r.t + CAST(ceil(CASE WHEN r.later_n >= 5 THEN r.later_p50 ELSE coalesce(l.late_p50, 0) END) AS INTEGER)
              ELSE greatest(r.planned_date + CAST(round(r.typical) AS INTEGER), r.t) END AS p50_date,
         CASE WHEN r.planned_date < r.t
              THEN r.t + CAST(ceil(CASE WHEN r.later_n >= 5 THEN r.later_p80 ELSE coalesce(l.late_p80, 0) END) AS INTEGER)
              ELSE greatest(r.planned_date + CAST(round(r.pessimistic) AS INTEGER), r.t) END AS p80_date
  FROM r LEFT JOIN late l ON l.t = r.t AND l.revision = r.revision`);

const table = (title, data) => { console.log(`\n${title}`); console.table(data); };

console.log(`Backtest de la proyección de fechas — tenant ${TENANT}`);
console.log(`Referencia: fecha PLANIFICADA + atraso histórico conocido en cada momento. Tolerancia de "tarde": > ${LATE_TOLERANCE_DAYS} días.`);
table('Muestra', await rows(`
  SELECT (SELECT count(*) FROM proj) AS proyectos_terminados,
         (SELECT count(DISTINCT project_id) FROM pend) AS proyectos_evaluados,
         (SELECT count(*) FROM pend) AS revisiones_pendientes_evaluadas,
         (SELECT count(*) FROM hist) AS emisiones_en_historia,
         CAST((SELECT min(t) FROM cp) AS VARCHAR) AS primer_corte, CAST((SELECT max(t) FROM cp) AS VARCHAR) AS ultimo_corte`));

table('1. Revisiones: error de la fecha proyectada (días). naive = usar la fecha planificada tal cual', await rows(`
  SELECT CAST(frac * 100 AS INTEGER) || '%' AS avance_del_plazo, count(*) AS revisiones,
         round(100.0 * count(*) FILTER (has_history) / count(*)) AS con_historia_pct,
         round(avg(abs(issued_date - naive_date)), 1) AS error_abs_naive,
         round(avg(abs(issued_date - p50_date)), 1) AS error_abs_tipico,
         round(median(issued_date - p50_date), 1) AS sesgo_mediano_tipico,
         round(100.0 * count(*) FILTER (issued_date <= p50_date) / count(*)) AS emitidas_antes_de_tipico_pct,
         round(100.0 * count(*) FILTER (issued_date <= p80_date) / count(*)) AS emitidas_antes_de_pesimista_pct
  FROM proj_rev GROUP BY frac ORDER BY frac`));

await con.run(`
  CREATE TABLE proj_cp AS
  SELECT project_id, frac, t, end_date, actual_finish,
         max(naive_date) AS naive_finish, max(p50_date) AS p50_finish, max(p80_date) AS p80_finish, count(*) AS pending
  FROM proj_rev GROUP BY ALL`);

table('2. Proyectos: error de la fecha de fin proyectada (días)', await rows(`
  SELECT CAST(frac * 100 AS INTEGER) || '%' AS avance_del_plazo, count(*) AS proyectos,
         round(avg(abs(actual_finish - naive_finish)), 1) AS error_abs_naive,
         round(avg(abs(actual_finish - p50_finish)), 1) AS error_abs_tipico,
         round(avg(abs(actual_finish - p80_finish)), 1) AS error_abs_pesimista,
         round(median(actual_finish - p50_finish), 1) AS sesgo_mediano_tipico,
         round(median(actual_finish - p80_finish), 1) AS sesgo_mediano_pesimista,
         round(100.0 * count(*) FILTER (actual_finish <= p80_finish) / count(*)) AS terminaron_antes_de_pesimista_pct
  FROM proj_cp GROUP BY frac ORDER BY frac`));

const classify = (label, predicted) => `
  SELECT '${label}' AS regla, CAST(frac * 100 AS INTEGER) || '%' AS avance_del_plazo, count(*) AS proyectos,
         count(*) FILTER (actual_finish > end_date + ${LATE_TOLERANCE_DAYS}) AS realmente_tarde,
         count(*) FILTER (${predicted}) AS marcados,
         count(*) FILTER (${predicted} AND actual_finish > end_date + ${LATE_TOLERANCE_DAYS}) AS aciertos,
         round(100.0 * count(*) FILTER (${predicted} AND actual_finish > end_date + ${LATE_TOLERANCE_DAYS}) / nullif(count(*) FILTER (${predicted}), 0)) AS precision_pct,
         round(100.0 * count(*) FILTER (${predicted} AND actual_finish > end_date + ${LATE_TOLERANCE_DAYS}) / nullif(count(*) FILTER (actual_finish > end_date + ${LATE_TOLERANCE_DAYS}), 0)) AS cobertura_pct
  FROM proj_cp GROUP BY frac`;
table('3. ¿Detecta los proyectos que terminan tarde? precision = de los marcados, cuántos se atrasaron; cobertura = de los atrasados, cuántos marcó', await rows(`
  SELECT * FROM (
    ${classify('naive: plan dice tarde', `naive_finish > end_date + ${LATE_TOLERANCE_DAYS}`)}
    UNION ALL ${classify('tipico dice tarde', `p50_finish > end_date + ${LATE_TOLERANCE_DAYS}`)}
    UNION ALL ${classify('pesimista dice tarde', `p80_finish > end_date + ${LATE_TOLERANCE_DAYS}`)}
  ) ORDER BY avance_del_plazo, regla`));

table('4. Atraso real de los proyectos evaluados (días después de la fecha de fin)', await rows(`
  SELECT count(*) AS proyectos, round(100.0 * count(*) FILTER (actual_finish > end_date + ${LATE_TOLERANCE_DAYS}) / count(*)) AS tarde_pct,
         round(median(actual_finish - end_date)) AS mediana, round(quantile_cont(actual_finish - end_date, 0.8)) AS p80, max(actual_finish - end_date) AS maximo
  FROM (SELECT DISTINCT project_id, actual_finish, end_date FROM proj_cp)`));
process.exit(0);
