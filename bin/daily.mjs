#!/usr/bin/env node
/**
 * Entry point del cron diario: corre export.mjs y, a continuación,
 * sync-postgres.mjs (Metabase lee de Postgres, no del lake directo — ver
 * README, sección "Metabase: materializado en Postgres").
 *
 * Si export.mjs termina con errores parciales (algún tenant falló, exit 1)
 * igual se sigue con sync-postgres — hay datos parciales en el lake que
 * vale la pena reflejar en Postgres, mismo criterio que export.mjs usa
 * entre tenants (no abortar todo por un fallo puntual). El exit code final
 * de este proceso es no-cero si CUALQUIERA de los dos pasos falló, para que
 * pm2/monitoring lo capture.
 */
import { execFileSync } from 'node:child_process';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const SCRIPT_DIR = dirname(fileURLToPath(import.meta.url));

function run(script) {
  execFileSync(process.execPath, [join(SCRIPT_DIR, script)], { stdio: 'inherit', env: process.env });
}

console.log(`\n=== daily lake job — ${new Date().toISOString()} ===`);

let exportFailed = false;
try {
  run('export.mjs');
} catch (err) {
  exportFailed = true;
  console.error(`\nexport.mjs terminó con errores (exit ${err.status ?? '?'}) — sigo con sync-postgres igual.`);
}

// sync-postgres solo alimenta una BI externa (Metabase) — opcional: corre
// únicamente si hay un Postgres configurado (PG_HOST).
let syncFailed = false;
if (process.env.PG_HOST) {
  try {
    run('sync-postgres.mjs');
  } catch (err) {
    syncFailed = true;
    console.error(`\nsync-postgres.mjs FALLÓ (exit ${err.status ?? '?'}).`);
  }
} else {
  console.log('\nPG_HOST no seteado — sync-postgres salteado.');
}

const failed = exportFailed || syncFailed;
console.log(`\n=== daily lake job ${failed ? 'CON ERRORES' : 'OK'} — ${new Date().toISOString()} ===`);
process.exit(failed ? 1 : 0);
