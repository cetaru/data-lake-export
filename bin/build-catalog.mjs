#!/usr/bin/env node
/**
 * Construye/actualiza un catálogo DuckDB persistido (un archivo .duckdb) con
 * vistas sobre el lake, para que Metabase (u otra herramienta BI) se conecte
 * directo sin conocer la estructura de particiones Hive.
 *
 * Las vistas usan `read_parquet(glob, hive_partitioning=true)` — DuckDB
 * resuelve el glob en tiempo de query, así que el catálogo **no hay que
 * reconstruirlo en cada export diario**: los snapshots nuevos aparecen solos.
 * Solo hace falta re-correr esto cuando se agrega/saca un dataset (colección
 * raw nueva, vista curada nueva).
 *
 * Por cada dataset se crean dos vistas:
 *   {name}         — histórico completo (todos los snapshots, todos los tenants)
 *   {name}_latest  — solo el snapshot_date más reciente **de cada tenant**
 *                    (no un max global: un tenant atrasado no debe opacar a
 *                    los demás ni el global debe ocultar el atraso de uno).
 *
 * IMPORTANTE — paths: el path embebido en cada vista es el que ve el proceso
 * que ABRE el catálogo (Metabase corriendo en Docker, o `duckdb` CLI en el
 * host). Para que el mismo .duckdb sirva en ambos casos sin reescribir
 * vistas, la convención es montar el lake en el container Docker en el
 * MISMO path absoluto que tiene en el host (ver metabase/docker-compose.yml).
 * Por eso LAKE_DIR se usa tal cual, sin traducir.
 *
 * Config (env):
 *   LAKE_DIR      raíz del lake, mismo valor usado en bin/export.mjs
 *                 (default /data/lake)
 *   CATALOG_PATH  archivo .duckdb de salida (default ./lake.duckdb)
 */
import { DuckDBInstance } from '@duckdb/node-api';
import { mkdir, rm } from 'node:fs/promises';
import { existsSync } from 'node:fs';
import { dirname } from 'node:path';
import { RAW_COLLECTIONS, RAW_VIEWS } from '../src/export-tenant.mjs';

const LAKE_DIR = process.env.LAKE_DIR || '/data/lake';
const CATALOG_PATH = process.env.CATALOG_PATH || './lake.duckdb';

const CURATED = ['evm_daily', 'evm_series'];

function latestViewSql(name) {
  return `
    CREATE OR REPLACE VIEW ${name}_latest AS
    SELECT t.*
    FROM ${name} t
    WHERE t.snapshot_date = (
      SELECT max(t2.snapshot_date) FROM ${name} t2 WHERE t2.tenant_id = t.tenant_id
    )
  `;
}

async function main() {
  await mkdir(dirname(CATALOG_PATH), { recursive: true });
  // DuckDB no soporta CREATE OR REPLACE VIEW cambiando el schema subyacente
  // de forma confiable entre corridas (ej. si cambia RAW_COLLECTIONS) — se
  // reconstruye el catálogo de cero cada vez que se corre este script.
  if (existsSync(CATALOG_PATH)) await rm(CATALOG_PATH);

  const instance = await DuckDBInstance.create(CATALOG_PATH);
  const con = await instance.connect();

  const datasets = [
    ...RAW_COLLECTIONS.map((name) => ({ name: `raw_${name}`, layer: 'raw', source: name })),
    ...RAW_VIEWS.map((name) => ({ name: `raw_${name}`, layer: 'raw', source: name })),
    ...CURATED.map((name) => ({ name: `curated_${name}`, layer: 'curated', source: name })),
  ];

  const created = [];
  const skipped = [];

  for (const { name, layer, source } of datasets) {
    const glob = `${LAKE_DIR}/${layer}/${source}/**/*.parquet`;
    try {
      await con.run(`
        CREATE OR REPLACE VIEW ${name} AS
        SELECT * FROM read_parquet('${glob}', hive_partitioning=true, union_by_name=true)
      `);
      await con.run(latestViewSql(name));
      created.push(name);
    } catch (err) {
      // dataset sin parquet todavía (colección vacía, o el export nunca corrió
      // para ese tenant) — no bloquea el resto del catálogo.
      skipped.push({ name, reason: String(err.message).split('\n')[0] });
    }
  }

  con.closeSync();

  console.log(`Catálogo escrito: ${CATALOG_PATH}`);
  console.log(`  vistas creadas: ${created.length} datasets × 2 (histórico + _latest)`);
  if (skipped.length) {
    console.log(`  omitidos (sin datos aún):`);
    for (const s of skipped) console.log(`    ${s.name} — ${s.reason}`);
  }
  console.log(`\nPaths embebidos en las vistas: ${LAKE_DIR}/... — asegurate de que quien abra`);
  console.log(`el catálogo (Metabase, duckdb CLI) vea el lake en ese mismo path.`);
}

main().catch((err) => { console.error('build-catalog FAILED:', err); process.exit(1); });
