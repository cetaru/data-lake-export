#!/usr/bin/env node
/**
 * Materializa el lake en Postgres para que Metabase (u otra BI) lo consulte
 * con su driver JDBC nativo, sin pasar por DuckDB embebido.
 *
 * Por qué: el driver comunitario de DuckDB para Metabase requiere su
 * librería nativa (glibc); la imagen oficial de Metabase es Alpine (musl) y
 * el binario nativo de DuckDB crashea (SIGSEGV) incluso con gcompat
 * instalado — probado en esta VPS, no es viable sin reconstruir la imagen
 * Docker con una base glibc. Postgres es JDBC puro, sin librería nativa:
 * cero riesgo de este problema, y Metabase ya lo tiene andando (mismo
 * docker-compose, container metabase-postgres).
 *
 * Qué escribe (todo vía la extensión `postgres` de DuckDB, ATTACH + CREATE
 * OR REPLACE TABLE — reescribe cada tabla entera en cada corrida, los datos
 * son chicos):
 *   - raw/{collection}  → tabla `{collection}` = solo el snapshot_date más
 *     reciente **de cada tenant** (estado actual, no arrastra historia
 *     completa a Postgres — eso vive en el lake Parquet).
 *   - curated/evm_daily, curated/evm_series → históricas completas (chicas,
 *     sirven para las curvas de tendencia).
 *
 * Config (env):
 *   LAKE_DIR      raíz del lake, mismo valor usado en bin/export.mjs
 *   PG_HOST / PG_PORT / PG_DATABASE / PG_USER / PG_PASSWORD
 */
import { DuckDBInstance } from '@duckdb/node-api';
import { RAW_COLLECTIONS, RAW_VIEWS } from '../src/export-tenant.mjs';

const LAKE_DIR = process.env.LAKE_DIR || '/data/lake';
const PG = {
  host: process.env.PG_HOST || '127.0.0.1',
  port: process.env.PG_PORT || '5432',
  database: process.env.PG_DATABASE || 'lake',
  user: process.env.PG_USER || 'metabase',
  password: process.env.PG_PASSWORD || 'metabase',
};

const CURATED = ['evm_daily', 'evm_series'];

function latestOfEachTenantSql(glob) {
  return `
    SELECT t.* FROM read_parquet('${glob}', hive_partitioning=true, union_by_name=true) t
    WHERE t.snapshot_date = (
      SELECT max(t2.snapshot_date)
      FROM read_parquet('${glob}', hive_partitioning=true, union_by_name=true) t2
      WHERE t2.tenant_id = t.tenant_id
    )
  `;
}

function fullHistorySql(glob) {
  return `SELECT * FROM read_parquet('${glob}', hive_partitioning=true, union_by_name=true)`;
}

async function main() {
  const instance = await DuckDBInstance.create(':memory:');
  const con = await instance.connect();
  await con.run(`INSTALL postgres; LOAD postgres;`);
  await con.run(
    `ATTACH 'host=${PG.host} port=${PG.port} dbname=${PG.database} user=${PG.user} password=${PG.password}' AS pg (TYPE postgres)`
  );

  const tables = [
    ...RAW_COLLECTIONS.map((name) => ({ table: name, glob: `${LAKE_DIR}/raw/${name}/**/*.parquet`, sql: latestOfEachTenantSql })),
    ...RAW_VIEWS.map((name) => ({ table: name, glob: `${LAKE_DIR}/raw/${name}/**/*.parquet`, sql: latestOfEachTenantSql })),
    ...CURATED.map((name) => ({ table: name, glob: `${LAKE_DIR}/curated/${name}/**/*.parquet`, sql: fullHistorySql })),
  ];

  const ok = [];
  const skipped = [];

  for (const { table, glob, sql } of tables) {
    try {
      await con.run(`CREATE OR REPLACE TABLE pg.public."${table}" AS ${sql(glob)}`);
      const rows = (await con.runAndReadAll(`SELECT count(*) c FROM pg.public."${table}"`)).getRows()[0][0];
      ok.push({ table, rows: Number(rows) });
    } catch (err) {
      skipped.push({ table, reason: String(err.message).split('\n')[0] });
    }
  }

  con.closeSync();

  console.log(`Sync a Postgres OK — ${PG.database}@${PG.host}:${PG.port}`);
  for (const t of ok) console.log(`  ${t.table.padEnd(30)} ${t.rows} filas`);
  if (skipped.length) {
    console.log('\nOmitidas (sin datos aún en el lake):');
    for (const s of skipped) console.log(`  ${s.table} — ${s.reason}`);
  }
}

main().catch((err) => { console.error('sync-postgres FAILED:', err); process.exit(1); });
