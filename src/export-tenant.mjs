/**
 * Export de un tenant: PocketBase SQLite → Parquet particionado (Hive-style).
 *
 *   raw/{collection}/tenant_id=X/snapshot_date=YYYY-MM-DD/data.parquet
 *   curated/evm_daily/tenant_id=X/snapshot_date=YYYY-MM-DD/data.parquet
 *   curated/evm_series/tenant_id=X/snapshot_date=YYYY-MM-DD/data.parquet
 *   curated/{delay_stats,risk_revisions,risk_daily}/... (ver src/risk.mjs)
 *
 * Portado 1:1 desde approcket/scripts/lake/export.mjs (rama
 * feature/data-lake-export), parametrizado para poder correr en loop sobre
 * varios tenants (un data.db por cliente, todos en la misma VPS).
 *
 * - Snapshot completo por corrida (no incremental). Si el volumen crece,
 *   cambiar a incremental filtrando por `updated > último snapshot` en el
 *   SELECT de cada colección (raw) y re-derivando curated.
 * - Idempotente: correr dos veces el mismo día para el mismo tenant reemplaza
 *   el snapshot del día (escribe a directorio temporal y renombra al final).
 * - Lectura SQLite en READ_ONLY: con WAL habilitado (default de PocketBase)
 *   los lectores no bloquean al writer, y read-only no puede corromper la DB.
 * - La lógica EVM NO se reimplementa acá: se carga el mismo módulo que usa
 *   el hook /pm2pro (approcket pb_hooks/lib/evm.js, ver src/evm-lib.mjs). El
 *   SQL corre sobre SQLite vía node:sqlite (mismo motor/dialecto que el hook,
 *   incluida strftime(...,'weekday 0')); DuckDB se usa para leer tablas y
 *   escribir Parquet.
 */
import { DuckDBInstance } from '@duckdb/node-api';
import { DatabaseSync } from 'node:sqlite';
import { mkdir, rm, rename, writeFile } from 'node:fs/promises';
import { existsSync } from 'node:fs';
import { join, dirname } from 'node:path';
import { loadEvmLib, toSqlite } from './evm-lib.mjs';
import { exportRisk } from './risk.mjs';

// Colecciones raw (ver README). users, config, pdf_templates, etc. quedan
// excluidas (PII / operativas / legacy).
export const RAW_COLLECTIONS = [
  // núcleo operativo y de avance
  'projects', 'clients', 'documents', 'document_review', 'document_forecast',
  'reviews', 'review_state', 'emitions', 'emition_items', 'emition_motive',
  'emition_state', 'document_type', 'document_links', 'base_line',
  'plan_hours', 'plan_rev',
  // horas y recursos
  'hours', 'specialties', 'seniority', 'holidays', 'days',
  'document_assignment', 'project_assignment',
  // financiero
  'budgets', 'budget_items', 'costs', 'costs_items', 'tariff', 'tariff_items',
  'credit_rules', 'credit_rules_items', 'transactions', 'type_transaction',
  // catálogos
  'origen_type', // ignored_count=1 marca documentos cancelados (fuera de los informes)
  // dispatch
  'dispatchs', 'dispatch_items',
];

// Vistas SQL con lógica de agregación valiosa. Se leen con node:sqlite (la
// extensión sqlite de DuckDB no parsea los backticks de estas vistas).
// list_document_forecast trae erev_a/b/0: fecha de emisión por umbral de
// crédito acumulado, el criterio de "revisión emitida" de la app.
export const RAW_VIEWS = ['list_hours', 'list_document_forecast', 'list_resume_hours'];

/**
 * Calcula evm_daily + evm_series para todos los proyectos de un tenant, con
 * las queries y la matemática de KPIs del módulo compartido con el hook
 * /pm2pro.
 * `sq` es una DatabaseSync read-only sobre el data.db del tenant.
 */
export function computeEvm(sq, snapshotDate) {
  const evm = loadEvmLib();
  const all = (sql, bind) => { const q = toSqlite(sql, bind); return sq.prepare(q.text).all(q.bind); };
  const get = (sql, bind) => { const q = toSqlite(sql, bind); return sq.prepare(q.text).get(q.bind); };

  const today = new Date().toISOString().slice(0, 10);
  const hoursCutoff = snapshotDate === today
    ? new Date().toISOString().slice(0, 19).replace('T', ' ')
    : `${snapshotDate} 23:59:59`;

  const projects = sq.prepare(`SELECT id, code, name FROM projects`).all();
  const daily = [];
  const series = [];

  for (const project of projects) {
    const cfg = get(evm.configSql, { project: project.id });
    const hoursActual = get(evm.hoursActualSql, { project: project.id, cutoff: hoursCutoff }).value;

    const isAB0 = cfg.code === 'AB0';
    const bind = evm.seriesBind(project.id, cfg);

    const cumBase = evm.accumulateByWeek(all(evm.plannedSql(isAB0), bind));
    const cumReal = evm.accumulateByWeek(all(evm.realSql(), bind));
    const cumActivity = evm.accumulateByWeek(all(evm.activitySql(), bind));

    const hoursOc = cfg.hours_oc ?? 0;
    const kpis = evm.computeKpis(cumBase, cumReal, hoursOc, hoursActual, snapshotDate);
    const activityLast = cumActivity.length > 0 ? cumActivity[cumActivity.length - 1].value : 0;

    daily.push({
      project_id: project.id,
      project_code: project.code,
      credit_rule_code: cfg.code,
      pct_a: bind.pct_a,
      pct_b: bind.pct_b,
      pct_0: bind.pct_0,
      hours_oc: hoursOc,
      pv_pct: kpis.planned_pct,
      ev_pct: kpis.real_pct,
      ev_activity_pct: activityLast,
      hours_actual: hoursActual,
      hours_planned: (kpis.planned_pct / 100) * hoursOc,
      hpi: kpis.hpi,
      spi: kpis.spi,
    });

    for (const [serie, cum] of [['planned', cumBase], ['real', cumReal], ['activity', cumActivity]]) {
      for (const p of cum) {
        series.push({ project_id: project.id, week: p.week, serie, value: p.value });
      }
    }
  }

  return { daily, series };
}

/**
 * Exporta un tenant completo (raw + curated) al lake.
 *
 * @param {object} opts
 * @param {string} opts.lakeDir      raíz del lake (ej. /data/lake)
 * @param {string} opts.pbData       path al data.db del tenant
 * @param {string} opts.tenantId     partición tenant (código de workspace)
 * @param {string} opts.snapshotDate YYYY-MM-DD
 * @returns {Promise<{tenantId: string, summary: Array, warnings: string[], seconds: number}>}
 */
export async function exportTenant({ lakeDir, pbData, tenantId, snapshotDate }) {
  const t0 = Date.now();
  const warnings = [];
  const summary = [];

  if (!existsSync(pbData)) {
    throw new Error(`no existe data.db para tenant "${tenantId}": ${pbData}`);
  }

  const tmpDir = join(lakeDir, `.tmp-export-${tenantId}-${Date.now()}`);
  await mkdir(tmpDir, { recursive: true });

  const instance = await DuckDBInstance.create(':memory:');
  const con = await instance.connect();
  await con.run(`SET memory_limit='2GB'`); // VPS compartida con PocketBase/Nuxt de todos los tenants
  await con.run(`INSTALL sqlite; LOAD sqlite;`);
  await con.run(`ATTACH '${pbData}' AS pb (TYPE sqlite, READ_ONLY)`);

  const sq = new DatabaseSync(pbData, { readOnly: true });
  const existing = new Set(
    sq.prepare(`SELECT name FROM sqlite_master WHERE type IN ('table','view')`)
      .all().map((r) => r.name)
  );

  const partition = (layer, name) =>
    join(tmpDir, layer, name, `tenant_id=${tenantId}`, `snapshot_date=${snapshotDate}`);

  const copyToParquet = async (layer, name, selectSql) => {
    const dir = partition(layer, name);
    await mkdir(dir, { recursive: true });
    await con.run(`COPY (${selectSql}) TO '${join(dir, 'data.parquet')}' (FORMAT parquet)`);
    const rows = (await con.runAndReadAll(`SELECT count(*) c FROM (${selectSql})`)).getRows()[0][0];
    summary.push({ name: `${layer}/${name}`, rows: Number(rows) });
  };

  try {
    // raw: tablas
    for (const coll of RAW_COLLECTIONS) {
      if (!existing.has(coll)) {
        warnings.push(`colección inexistente: ${coll} — omitida`);
        continue;
      }
      await copyToParquet('raw', coll, `SELECT * FROM pb."${coll}"`);
    }

    // raw: vistas — leídas con node:sqlite y pasadas a Parquet vía NDJSON
    for (const view of RAW_VIEWS) {
      if (!existing.has(view)) {
        warnings.push(`vista inexistente: ${view} — omitida`);
        continue;
      }
      try {
        const rows = sq.prepare(`SELECT * FROM "${view}"`).all();
        if (!rows.length) { warnings.push(`vista ${view}: sin filas — omitida`); continue; }
        const jsonPath = join(tmpDir, `${view}.json`);
        await writeFile(jsonPath, rows.map((r) => JSON.stringify(r)).join('\n'));
        // sample_size=-1: tipos inferidos sobre todas las filas, no solo una muestra
        await copyToParquet('raw', view, `SELECT * FROM read_json_auto('${jsonPath}', format='newline_delimited', sample_size=-1)`);
        await rm(jsonPath);
      } catch (err) {
        warnings.push(`vista ${view} no exportada (${String(err.message).split('\n')[0]})`);
      }
    }

    // curated: evm_daily + evm_series
    const { daily, series } = computeEvm(sq, snapshotDate);
    const writeJsonParquet = async (name, rows, columns) => {
      if (!rows.length) { warnings.push(`${name}: sin filas`); return; }
      const jsonPath = join(tmpDir, `${name}.json`);
      await writeFile(jsonPath, rows.map((r) => JSON.stringify(r)).join('\n'));
      await copyToParquet('curated', name, `SELECT ${columns.join(', ')} FROM read_json_auto('${jsonPath}')`);
      await rm(jsonPath);
    };
    await writeJsonParquet('evm_daily', daily, [
      'project_id', 'project_code', 'credit_rule_code', 'pct_a', 'pct_b', 'pct_0', 'hours_oc', 'pv_pct',
      'ev_pct', 'ev_activity_pct', 'hours_actual', 'hours_planned', 'hpi', 'spi',
    ]);
    await writeJsonParquet('evm_series', series, ['project_id', 'week', 'serie', 'value']);

    // curated: schedule risk (delay statistics, projected dates, project score)
    const tmpParquet = (layer, name) => {
      const file = join(partition(layer, name), 'data.parquet');
      return existsSync(file) ? file : null;
    };
    try {
      warnings.push(...await exportRisk({
        con, snapshotDate, copyToParquet,
        rawParquet: (name) => tmpParquet('raw', name),
        evmDailyParquet: tmpParquet('curated', 'evm_daily'),
      }));
    } catch (err) {
      warnings.push(`riesgo no calculado (${String(err.message).split('\n')[0]})`);
    }
  } finally {
    sq.close();
    con.closeSync();
  }

  // mover snapshot atómicamente: tmp/{layer}/{name}/... → lake/{layer}/{name}/...
  for (const { name } of summary) {
    const src = join(tmpDir, name, `tenant_id=${tenantId}`, `snapshot_date=${snapshotDate}`);
    const dst = join(lakeDir, name, `tenant_id=${tenantId}`, `snapshot_date=${snapshotDate}`);
    await mkdir(dirname(dst), { recursive: true });
    await rm(dst, { recursive: true, force: true });
    await rename(src, dst);
  }
  await rm(tmpDir, { recursive: true, force: true });

  return { tenantId, summary, warnings, seconds: (Date.now() - t0) / 1000 };
}

/**
 * Extrae las definiciones de vistas SQL del origen de un tenant, como
 * documentación/referencia para la capa curada (no crítico si falla).
 */
export function dumpViewDefinitions(pbData) {
  const sq = new DatabaseSync(pbData, { readOnly: true });
  try {
    return sq.prepare(
      `SELECT name, sql FROM sqlite_master WHERE type='view' ORDER BY name`
    ).all();
  } finally {
    sq.close();
  }
}
