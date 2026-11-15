/**
 * Loads the EVM module shared with approcket's PocketBase hooks
 * (pb_hooks/lib/evm.js) — the same file the live /pm2pro endpoint uses, so
 * the lake cannot drift from the app when the hook logic changes.
 *
 * The file is CommonJS written for PocketBase's JSVM and lives inside a
 * package with "type": "module", so Node cannot require() it directly; it is
 * evaluated here with a CommonJS-style wrapper instead.
 *
 * EVM_LIB_PATH  path to pb_hooks/lib/evm.js
 *               (default: ../approcket/pocketbase/pb_hooks/lib/evm.js, next
 *               to this repo; in production point it at the hooks dir the
 *               PocketBase instances run with)
 */
import { readFileSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const HERE = dirname(fileURLToPath(import.meta.url));
const DEFAULT_PATH = join(HERE, '..', '..', 'approcket', 'pocketbase', 'pb_hooks', 'lib', 'evm.js');

export function loadEvmLib() {
  const path = resolve(process.env.EVM_LIB_PATH || DEFAULT_PATH);
  let source;
  try {
    source = readFileSync(path, 'utf8');
  } catch (err) {
    throw new Error(`no se pudo leer el módulo EVM compartido (${path}) — setear EVM_LIB_PATH: ${err.message}`);
  }
  const module = { exports: {} };
  new Function('module', 'exports', source)(module, module.exports);
  return module.exports;
}

/**
 * PocketBase named params ({:name}) → SQLite named params (:name), plus the
 * subset of `bind` the statement actually uses (node:sqlite rejects unknown
 * named parameters).
 */
export function toSqlite(sql, bind) {
  const used = {};
  const text = sql.replace(/\{:(\w+)\}/g, (_, name) => {
    if (!(name in bind)) throw new Error(`falta el parámetro "${name}" para la query EVM`);
    used[name] = bind[name];
    return `:${name}`;
  });
  return { text, bind: used };
}
