/**
 * Hands today's project risk to the TrakSync app.
 *
 * The app reads PocketBase, the risk lives in the lake. Instead of giving the
 * exporter write credentials to every tenant's PocketBase, it drops a small
 * JSON file inside that tenant's own pb_data directory:
 *
 *   <pb_data>/lake/risk_daily.json
 *
 * The app's /dashboard hook reads it from its data directory. Tenant
 * isolation is the directory itself; no token, no network call.
 *
 * `previous_risk_level` comes from the tenant's previous lake snapshot, so
 * the app can point out projects that just moved to high risk.
 */
import { mkdir, writeFile, rename } from 'node:fs/promises';
import { existsSync, readdirSync } from 'node:fs';
import { dirname, join } from 'node:path';

function previousSnapshot(lakeDir, tenantId, snapshotDate) {
  const dir = join(lakeDir, 'curated', 'risk_daily', `tenant_id=${tenantId}`);
  if (!existsSync(dir)) return null;
  const date = readdirSync(dir)
    .map((d) => /^snapshot_date=(\d{4}-\d{2}-\d{2})$/.exec(d)?.[1])
    .filter((d) => d && d < snapshotDate).sort().at(-1);
  const file = date && join(dir, `snapshot_date=${date}`, 'data.parquet');
  return file && existsSync(file) ? { date, file } : null;
}

/**
 * @returns {Promise<string|null>} path written, or null when skipped
 */
export async function writeAppRiskFeed({ con, riskParquet, lakeDir, tenantId, snapshotDate, pbData }) {
  // Only the current day feeds the app: a backfill of a past date must not overwrite it.
  if (snapshotDate !== new Date().toISOString().slice(0, 10)) return null;

  const previous = previousSnapshot(lakeDir, tenantId, snapshotDate);
  const previousJoin = previous
    ? `LEFT JOIN read_parquet('${previous.file}') p ON p.project_id = r.project_id`
    : '';
  const reader = await con.runAndReadAll(`
    SELECT r.project_id, r.risk_level, r.risk_score,
           ${previous ? 'p.risk_level' : 'CAST(NULL AS VARCHAR)'} AS previous_risk_level,
           ${previous ? 'p.risk_score' : 'CAST(NULL AS INTEGER)'} AS previous_risk_score,
           CAST(r.end_date AS VARCHAR) AS end_date,
           CAST(r.projected_finish_p50 AS VARCHAR) AS projected_finish_p50,
           CAST(r.projected_finish_p80 AS VARCHAR) AS projected_finish_p80,
           r.slip_days_p50, r.slip_days_p80, r.spi, r.pv_pct, r.ev_pct,
           r.pending_revisions, r.overdue_revisions, r.stale_revisions, r.overdue_share_pct,
           r.hours_budget, r.hours_at_completion, r.hours_overrun_pct,
           r.weekly_hours_last_4w, r.remaining_hours, CAST(r.capacity_finish AS VARCHAR) AS capacity_finish
    FROM read_parquet('${riskParquet}') r ${previousJoin}`);

  const projects = {};
  for (const row of reader.getRowObjectsJson()) {
    const { project_id: id, ...rest } = row;
    for (const [key, value] of Object.entries(rest)) {
      // integers come back as strings from DuckDB's JSON conversion
      if (typeof value === 'string' && /^-?\d+$/.test(value)) rest[key] = Number(value);
    }
    projects[id] = rest;
  }

  const target = join(dirname(pbData), 'lake', 'risk_daily.json');
  await mkdir(dirname(target), { recursive: true });
  const payload = {
    snapshot_date: snapshotDate,
    previous_snapshot_date: previous?.date ?? null,
    generated_at: new Date().toISOString(),
    projects,
  };
  await writeFile(`${target}.tmp`, JSON.stringify(payload));
  await rename(`${target}.tmp`, target); // atomic: the app never reads a half-written file
  return target;
}
