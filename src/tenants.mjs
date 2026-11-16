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
 * Alternativa sin tocar pb_ws: PB_DATA_TEMPLATE, un path con `{tenant}` que
 * se resuelve con el código del workspace cuando no tiene `pb_data_path`
 * (ej. /root/sites/traksync.io/pb-workspaces/{tenant}/pb_data/data.db). Los
 * workspaces cuyo data.db no existe en esta VPS (ej. uno hosteado afuera) se
 * omiten con un aviso.
 *
 * Todos los tenants corren en la misma VPS que este exporter, así que se lee
 * el data.db directo por filesystem (mismo approach que el export single-
 * tenant: ATTACH ... TYPE sqlite READ_ONLY) — no hay llamadas a la API en
 * vivo de cada PocketBase, cero carga extra sobre los tenants.
 */
import PocketBase from 'pocketbase';
import { existsSync } from 'node:fs';

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

  const template = process.env.PB_DATA_TEMPLATE || '';
  const tenants = [];
  const skipped = [];
  const missing = [];
  for (const ws of workspaces) {
    if (ws.active === false) continue; // si el campo existe y está apagado
    const pbData = ws.pb_data_path || (template ? template.replaceAll('{tenant}', ws.code) : '');
    if (!pbData) {
      skipped.push(ws.code);
      continue;
    }
    if (!ws.pb_data_path && !existsSync(pbData)) {
      missing.push(ws.code); // path derivado del template: el tenant no vive en esta VPS
      continue;
    }
    tenants.push({ tenantId: ws.code, pbData, name: ws.name ?? ws.code });
  }

  if (skipped.length) {
    console.warn(`[tenants] sin pb_data_path (y sin PB_DATA_TEMPLATE), omitidos: ${skipped.join(', ')}`);
  }
  if (missing.length) {
    console.warn(`[tenants] sin data.db en esta VPS, omitidos: ${missing.join(', ')}`);
  }

  return tenants;
}
