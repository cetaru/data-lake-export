#!/usr/bin/env node
/**
 * Export de todos los tenants (loop sobre `workspaces` en pb_ws) o de un
 * único tenant manual (para dev/backfill).
 *
 * Modo multi-tenant (default, producción):
 *   node bin/export.mjs
 *   → lee tenants desde pb_ws (PB_WS_URL, PB_WS_ADMIN_EMAIL/PASSWORD)
 *
 * Modo single-tenant manual (dev/test contra una instancia pb_* local):
 *   PB_DATA=../pb/pb_tsp/pb_data/data.db TENANT_ID=tsp node bin/export.mjs
 *   → salta pb_ws por completo
 *
 * Config común (env):
 *   LAKE_DIR       destino del lake       (default /data/lake)
 *   SNAPSHOT_DATE  YYYY-MM-DD             (default hoy UTC; override para backfill)
 *
 * Un tenant que falla (data.db ausente/corrupto, colección faltante grave,
 * etc.) no aborta la corrida completa — se loggea y se sigue con el resto.
 */
import { writeFile, mkdir } from 'node:fs/promises';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import { exportTenant, dumpViewDefinitions } from '../src/export-tenant.mjs';
import { fetchTenants } from '../src/tenants.mjs';

const SCRIPT_DIR = dirname(fileURLToPath(import.meta.url));

const CONFIG = {
  lakeDir: process.env.LAKE_DIR || '/data/lake',
  snapshotDate: process.env.SNAPSHOT_DATE || new Date().toISOString().slice(0, 10),
};

async function resolveTenants() {
  if (process.env.PB_DATA && process.env.TENANT_ID) {
    return [{ tenantId: process.env.TENANT_ID, pbData: process.env.PB_DATA, name: process.env.TENANT_ID }];
  }
  return fetchTenants();
}

async function main() {
  const { lakeDir, snapshotDate } = CONFIG;
  const tenants = await resolveTenants();

  if (!tenants.length) {
    console.error('No hay tenants para exportar (revisar pb_ws / workspaces.pb_data_path, o PB_DATA+TENANT_ID para modo manual).');
    process.exit(1);
  }

  console.log(`Exportando ${tenants.length} tenant(s) — snapshot ${snapshotDate}\n`);

  const results = [];
  let anyFailed = false;

  for (const tenant of tenants) {
    process.stdout.write(`— ${tenant.tenantId} (${tenant.name})… `);
    try {
      const result = await exportTenant({ lakeDir, pbData: tenant.pbData, tenantId: tenant.tenantId, snapshotDate });
      console.log(`OK (${result.seconds.toFixed(1)}s, ${result.summary.length} datasets)`);
      if (result.warnings.length) {
        for (const w of result.warnings) console.log(`    ⚠ ${w}`);
      }
      results.push(result);

      // documentación: definiciones de vistas del origen (una copia por
      // tenant, no crítico si falla)
      try {
        const viewDefs = dumpViewDefinitions(tenant.pbData);
        await mkdir(join(SCRIPT_DIR, '..', 'view-definitions'), { recursive: true });
        await writeFile(
          join(SCRIPT_DIR, '..', 'view-definitions', `${tenant.tenantId}.sql`),
          `-- Definiciones de vistas de PocketBase — tenant ${tenant.tenantId}, ${snapshotDate}\n\n` +
          viewDefs.map((v) => `-- ${v.name}\n${v.sql};\n`).join('\n')
        );
      } catch (err) {
        console.log(`    ⚠ view-definitions no generado: ${err.message}`);
      }
    } catch (err) {
      anyFailed = true;
      console.log(`FALLÓ — ${err.message}`);
    }
  }

  const totalRows = results.reduce((acc, r) => acc + r.summary.reduce((a, s) => a + s.rows, 0), 0);
  console.log(`\nExport ${anyFailed ? 'con errores' : 'OK'} — ${results.length}/${tenants.length} tenants, ${totalRows} filas totales.`);
  process.exit(anyFailed ? 1 : 0);
}

main().catch((err) => { console.error('Export FAILED:', err); process.exit(1); });
