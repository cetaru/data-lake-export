/**
 * Registro de tenants: lee la colección `workspaces` de la PocketBase de
 * control (pb_ws — la misma que usa approcket/plugins/pocketbase.js para
 * resolver el workspace en el login).
 *
 * Requiere un campo nuevo en `workspaces` (no existe todavía — agregarlo con
 * una migración en el repo de control-plane, no en este repo):
 *
 *   pb_data_path  text   path absoluto al data.db de ese tenant en esta VPS
 *                        (ej. /home/.../code/pb/pb_tsp/pb_data/data.db)
 *
 * Todos los tenants corren en la misma VPS que este exporter, así que se lee
 * el data.db directo por filesystem (mismo approach que el export single-
 * tenant: ATTACH ... TYPE sqlite READ_ONLY) — no hay llamadas a la API en
 * vivo de cada PocketBase, cero carga extra sobre los tenants.
 */
import PocketBase from 'pocketbase';

/**
 * @returns {Promise<Array<{tenantId: string, pbData: string, name: string}>>}
 */
export async function fetchTenants() {
  const url = process.env.PB_WS_URL || 'http://localhost:8190';
  const pb = new PocketBase(url);
  pb.autoCancellation(false);

  const email = process.env.PB_WS_ADMIN_EMAIL;
  const password = process.env.PB_WS_ADMIN_PASSWORD;
  if (email && password) {
    await pb.collection('_superusers').authWithPassword(email, password);
  }

  const workspaces = await pb.collection('workspaces').getFullList({ sort: 'code' });

  const tenants = [];
  const skipped = [];
  for (const ws of workspaces) {
    if (ws.active === false) continue; // si el campo existe y está apagado
    if (!ws.pb_data_path) {
      skipped.push(ws.code);
      continue;
    }
    tenants.push({ tenantId: ws.code, pbData: ws.pb_data_path, name: ws.name ?? ws.code });
  }

  if (skipped.length) {
    console.warn(`[tenants] sin pb_data_path, omitidos: ${skipped.join(', ')}`);
  }

  return tenants;
}
