/**
 * Export de un tenant: PocketBase SQLite → Parquet particionado (Hive-style).
 *
 *   raw/{collection}/tenant_id=X/snapshot_date=YYYY-MM-DD/data.parquet
 *   curated/evm_daily/tenant_id=X/snapshot_date=YYYY-MM-DD/data.parquet
 *   curated/evm_series/tenant_id=X/snapshot_date=YYYY-MM-DD/data.parquet
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
 * - La lógica EVM se porta 1:1 de pb_hooks/get.pm2pro.pb.js de cada instancia
 *   (mismo hook en todos los tenants — misma versión de approcket). El SQL
 *   corre sobre SQLite vía node:sqlite (mismo motor/dialecto que el hook,
 *   incluida strftime(...,'weekday 0')) para que la validación contra
 *   /pm2pro/{project} cierre exacto; DuckDB se usa para leer tablas y
 *   escribir Parquet.
 */
import { DuckDBInstance } from '@duckdb/node-api';
import { DatabaseSync } from 'node:sqlite';
import { mkdir, rm, rename, writeFile } from 'node:fs/promises';
import { existsSync } from 'node:fs';
import { join, dirname } from 'node:path';

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
  // dispatch
  'dispatchs', 'dispatch_items',
];

// Vistas SQL con lógica de agregación valiosa — se exportan si la extensión
// sqlite de DuckDB puede leerlas.
export const RAW_VIEWS = ['list_hours', 'list_document_forecast', 'list_resume_hours'];

// ─── EVM (portado 1:1 de pb_hooks/get.pm2pro.pb.js) ──────────────────────────

function interpolateY(data, x2) {
  const x2Time = new Date(x2).getTime();
  const times = data.map((p) => new Date(p.week).getTime());
  for (let i = 0; i < data.length - 1; i++) {
    if (x2Time >= times[i] && x2Time <= times[i + 1]) {
      const p1 = data[i], p2 = data[i + 1];
      return parseFloat(p1.value) +
        (parseFloat(p2.value) - parseFloat(p1.value)) *
        ((x2Time - times[i]) / (times[i + 1] - times[i]));
    }
  }
  return data.length > 0 ? parseFloat(data[data.length - 1].value) : null;
}

function accumulateByWeek(rows) {
  const grouped = {};
  for (const r of rows) grouped[r.week] = (grouped[r.week] || 0) + r.value;
  const weeks = Object.keys(grouped).sort();
  let acc = 0;
  return weeks.map((week) => {
    acc += grouped[week];
    return { week, value: parseFloat(acc.toFixed(2)) };
  });
}

const startRow = `SELECT 0 as value, strftime('%Y-%m-%d', :start_date) as week`;

function plannedSql(isAB0) {
  return isAB0
    ? `${startRow}
       UNION ALL SELECT sum(weight*.7*100),  strftime('%Y-%m-%d',rev_a,'weekday 0') FROM documents WHERE project=:project AND rev_a<>'' GROUP BY 2
       UNION ALL SELECT sum(weight*.15*100), strftime('%Y-%m-%d',rev_b,'weekday 0') FROM documents WHERE project=:project AND rev_b<>'' GROUP BY 2
       UNION ALL SELECT sum(weight*.15*100), strftime('%Y-%m-%d',rev_0,'weekday 0') FROM documents WHERE project=:project AND rev_0<>'' GROUP BY 2
       ORDER BY 2`
    : `${startRow}
       UNION ALL SELECT sum(weight*.7*100), strftime('%Y-%m-%d',rev_a,'weekday 0') FROM documents WHERE project=:project AND rev_a<>'' GROUP BY 2
       UNION ALL SELECT sum(weight*.3*100), strftime('%Y-%m-%d',rev_0,'weekday 0') FROM documents WHERE project=:project AND rev_0<>'' GROUP BY 2
       ORDER BY 2`;
}

function realSql(isAB0) {
  const rev = (r) =>
    `UNION ALL SELECT sum(weight * real_credit), strftime('%Y-%m-%d', date, 'weekday 0') FROM document_review WHERE project=:project AND review='${r}' AND date<>'' GROUP BY 2`;
  return isAB0
    ? `${startRow}\n${rev('A')}\n${rev('B')}\n${rev('0')}\nORDER BY 2`
    : `${startRow}\n${rev('A')}\n${rev('0')}\nORDER BY 2`;
}

function activitySql(isAB0) {
  // El hook usa la vista list_documents para hs_total; node:sqlite la lee igual.
  const rev = (r) =>
    `UNION ALL SELECT sum((cast(hs_total as real) / :hours_oc) * real_credit), strftime('%Y-%m-%d', date, 'weekday 0') FROM document_review INNER JOIN list_documents ON document_review.document = list_documents.id WHERE document_review.project=:project AND review='${r}' AND date<>'' GROUP BY 2`;
  return isAB0
    ? `${startRow}\n${rev('A')}\n${rev('B')}\n${rev('0')}\nORDER BY 2`
    : `${startRow}\n${rev('A')}\n${rev('0')}\nORDER BY 2`;
}

/**
 * Calcula evm_daily + evm_series para todos los proyectos de un tenant.
 * `sq` es una DatabaseSync read-only sobre el data.db del tenant.
 */
export function computeEvm(sq, snapshotDate) {
  const today = new Date().toISOString().slice(0, 10);
  const hoursCutoff = snapshotDate === today
    ? new Date().toISOString().slice(0, 19).replace('T', ' ')
    : `${snapshotDate} 23:59:59`;

  const projects = sq.prepare(`SELECT id, code, name FROM projects`).all();
  const daily = [];
  const series = [];

  for (const project of projects) {
    const cfg = sq.prepare(`
      SELECT ifnull(cr.code, '') as code,
             ifnull(p.start_date, '') as start_date,
             (SELECT sum(hs_pas+hs_cad+hs_jr+hs_ssr+hs_sr)
              FROM documents WHERE project = p.id) as hours_oc
      FROM projects p
      LEFT JOIN credit_rules cr ON p.credit_rule = cr.id
      WHERE p.id = :project
    `).get({ project: project.id });

    const hoursActual = sq.prepare(`
      SELECT ifnull(sum(h.hours), 0) as value
      FROM hours h
      INNER JOIN documents d ON h.document = d.id
      INNER JOIN specialties s ON h.specialty = s.id
      WHERE d.project = :project
        AND h.date <= :cutoff
        AND s.ignored_count = 0
    `).get({ project: project.id, cutoff: hoursCutoff }).value;

    const isAB0 = cfg.code === 'AB0';
    const bindBase = { project: project.id, start_date: cfg.start_date };

    const rowsBase = sq.prepare(plannedSql(isAB0)).all(bindBase);
    const rowsReal = sq.prepare(realSql(isAB0)).all(bindBase);
    const rowsActivity = sq.prepare(activitySql(isAB0))
      .all({ ...bindBase, hours_oc: cfg.hours_oc || 1 });

    const clean = (rows) => rows.filter((r) => r.value !== null).map((r) => ({ week: r.week, value: Number(r.value) }));
    const cumBase = accumulateByWeek(clean(rowsBase));
    const cumReal = accumulateByWeek(clean(rowsReal));
    const cumActivity = accumulateByWeek(clean(rowsActivity));

    const plannedPct = interpolateY(cumBase, snapshotDate);
    const plannedValid = plannedPct && isFinite(plannedPct) && plannedPct > 0 ? plannedPct : 0;
    const realLast = cumReal.length > 0 ? parseFloat(cumReal[cumReal.length - 1].value) : 0;
    const activityLast = cumActivity.length > 0 ? parseFloat(cumActivity[cumActivity.length - 1].value) : 0;

    // HPI = PV/AC en horas (así lo calcula el hook; no es EV/AC — se porta
    // tal cual para que la validación contra /pm2pro cierre)
    let hpi = hoursActual > 0 && plannedValid > 0 && cfg.hours_oc > 0
      ? ((plannedValid / 100) * cfg.hours_oc) / hoursActual : 0;
    if (!isFinite(hpi)) hpi = 0;
    let spi = realLast > 0 && plannedValid > 0 ? realLast / plannedValid : 0;
    if (!isFinite(spi)) spi = 0;

    daily.push({
      project_id: project.id,
      project_code: project.code,
      credit_rule_code: cfg.code,
      hours_oc: cfg.hours_oc ?? 0,
      pv_pct: plannedValid,
      ev_pct: realLast,
      ev_activity_pct: activityLast,
      hours_actual: hoursActual,
      hours_planned: (plannedValid / 100) * (cfg.hours_oc ?? 0),
      hpi,
      spi,
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

    // raw: vistas
    for (const view of RAW_VIEWS) {
      if (!existing.has(view)) {
        warnings.push(`vista inexistente: ${view} — omitida`);
        continue;
      }
      try {
        await copyToParquet('raw', view, `SELECT * FROM pb."${view}"`);
      } catch (err) {
        warnings.push(`vista ${view} no legible vía DuckDB (${String(err.message).split('\n')[0]}) — TODO: reimplementar en capa curada`);
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
      'project_id', 'project_code', 'credit_rule_code', 'hours_oc', 'pv_pct',
      'ev_pct', 'ev_activity_pct', 'hours_actual', 'hours_planned', 'hpi', 'spi',
    ]);
    await writeJsonParquet('evm_series', series, ['project_id', 'week', 'serie', 'value']);
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
