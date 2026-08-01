#!/usr/bin/env node
/**
 * Validación del export para UN tenant (spot-check manual):
 *   1. Corre el export de ese tenant.
 *   2. Compara conteos de filas raw vs. SQLite origen.
 *   3. Imprime evm_daily del proyecto piloto (VALIDATE_PROJECT_CODE; si no
 *      se setea, usa el proyecto con más document_review).
 *   4. Compara hpi/spi/hours_oc contra el hook /pm2pro/{project} en vivo de
 *      ese tenant (PB_URL).
 *
 * Requiere PB_DATA + TENANT_ID (mismo tenant que se quiere inspeccionar).
 * Para validar todos los tenants, correr esto en loop desde afuera (ej. un
 * script de CI) — se mantiene single-tenant a propósito para que la salida
 * sea legible.
 */
import { DatabaseSync } from 'node:sqlite';
import { DuckDBInstance } from '@duckdb/node-api';
import { join } from 'node:path';
import { exportTenant, RAW_COLLECTIONS } from '../src/export-tenant.mjs';

const PB_DATA = process.env.PB_DATA;
const TENANT_ID = process.env.TENANT_ID;
const LAKE_DIR = process.env.LAKE_DIR || '/data/lake';
const SNAPSHOT_DATE = process.env.SNAPSHOT_DATE || new Date().toISOString().slice(0, 10);
const PB_URL = process.env.PB_URL || 'http://localhost:8090';
const PROJECT_CODE = process.env.VALIDATE_PROJECT_CODE || null;
const TOLERANCE = 1e-6;

if (!PB_DATA || !TENANT_ID) {
  console.error('Uso: PB_DATA=<path/data.db> TENANT_ID=<code> node bin/validate.mjs');
  process.exit(1);
}

let failed = false;
const check = (ok, label, detail = '') => {
  console.log(`${ok ? '✓' : '✗'} ${label}${detail ? ` — ${detail}` : ''}`);
  if (!ok) failed = true;
};

console.log(`— Corriendo export de "${TENANT_ID}"…`);
const result = await exportTenant({ lakeDir: LAKE_DIR, pbData: PB_DATA, tenantId: TENANT_ID, snapshotDate: SNAPSHOT_DATE });
console.log(`  OK (${result.seconds.toFixed(1)}s)`);

const sq = new DatabaseSync(PB_DATA, { readOnly: true });
const existing = new Set(
  sq.prepare(`SELECT name FROM sqlite_master WHERE type IN ('table','view')`).all().map((r) => r.name)
);
const instance = await DuckDBInstance.create(':memory:');
const con = await instance.connect();

console.log('\n— Conteos raw vs. SQLite:');
for (const coll of RAW_COLLECTIONS) {
  if (!existing.has(coll)) continue;
  const src = sq.prepare(`SELECT count(*) c FROM "${coll}"`).get().c;
  const parquet = join(LAKE_DIR, 'raw', coll, `tenant_id=${TENANT_ID}`, `snapshot_date=${SNAPSHOT_DATE}`, 'data.parquet');
  const dst = Number((await con.runAndReadAll(`SELECT count(*) FROM read_parquet('${parquet}')`)).getRows()[0][0]);
  check(src === dst, coll, `sqlite=${src} parquet=${dst}`);
}

const project = PROJECT_CODE
  ? sq.prepare(`SELECT id, code, name FROM projects WHERE code = :code`).get({ code: PROJECT_CODE })
  : sq.prepare(`
      SELECT p.id, p.code, p.name FROM projects p
      JOIN document_review dr ON dr.project = p.id
      GROUP BY p.id ORDER BY count(*) DESC LIMIT 1
    `).get();

if (!project) {
  console.log(`\n⚠ ${PROJECT_CODE ? `Proyecto ${PROJECT_CODE} no existe en esta DB` : 'No hay proyectos con reviews'} — salteando validación EVM.`);
} else {
  const evmParquet = join(LAKE_DIR, 'curated', 'evm_daily', `tenant_id=${TENANT_ID}`, `snapshot_date=${SNAPSHOT_DATE}`, 'data.parquet');
  const reader = await con.runAndReadAll(
    `SELECT * FROM read_parquet('${evmParquet}') WHERE project_id = '${project.id}'`
  );
  const cols = reader.columnNames();
  const row = reader.getRows()[0];
  console.log(`\n— evm_daily ${project.code} (${project.name}), tenant ${TENANT_ID}, snapshot ${SNAPSHOT_DATE}:`);
  cols.forEach((c, i) => console.log(`  ${c.padEnd(18)} ${row?.[i]}`));

  console.log(`\n— Hook /pm2pro/${project.id} (${PB_URL}):`);
  try {
    const res = await fetch(`${PB_URL}/pm2pro/${project.id}`, { signal: AbortSignal.timeout(10000) });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const hook = await res.json();
    const evm = Object.fromEntries(cols.map((c, i) => [c, Number(row[i])]));
    for (const [key, hookVal] of [['hpi', hook.hpi], ['spi', hook.spi], ['hours_oc', hook.hours_oc]]) {
      const diff = Math.abs(evm[key] - Number(hookVal));
      check(diff < TOLERANCE, `${key} lake=${evm[key]} hook=${hookVal}`, `diff=${diff}`);
    }
  } catch (err) {
    console.log(`  ⚠ Hook no accesible (${err.message}) — comparación salteada.`);
    console.log('    Levantar la PocketBase del tenant o setear PB_URL para la validación completa.');
  }
}

sq.close();
con.closeSync();
console.log(failed ? '\nVALIDACIÓN CON ERRORES' : '\nValidación OK');
process.exit(failed ? 1 : 0);
