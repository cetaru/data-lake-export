# data-lake-export

Export batch diario, **multi-tenant**, de PocketBase (SQLite) a Parquet
particionado, con DuckDB. Alimenta dashboards de tendencia (EVM, portfolio);
lo operativo de cada cliente sigue consultando su PocketBase en vivo.

Repo independiente de `approcket` (la app Nuxt) — corre como proceso propio
en la misma VPS que todas las instancias PocketBase de los clientes, así que
lee cada `data.db` directo por filesystem (sin pegarle a ninguna API en vivo).

## Cómo sabe qué tenants exportar

Lee la colección `workspaces` de la PocketBase de control (`pb_ws`, la misma
que usa `approcket/plugins/pocketbase.js` para resolver el workspace en el
login). Cada registro de `workspaces` necesita un campo:

```
pb_data_path   text   path absoluto al data.db del tenant en esta VPS
                       (ej. /home/.../code/pb/pb_tsp/pb_data/data.db)
```

**Este campo no existe todavía** — hay que agregarlo con una migración en el
repo de control-plane de `pb_ws` (fuera de este repo). Hasta que exista, el
export multi-tenant no encuentra tenants (loggea warning y sale con error).

## Estructura del lake

```
/data/lake/
  raw/
    {collection}/tenant_id={tenant}/snapshot_date=YYYY-MM-DD/data.parquet
  curated/
    evm_daily/tenant_id={tenant}/snapshot_date=YYYY-MM-DD/data.parquet
    evm_series/tenant_id={tenant}/snapshot_date=YYYY-MM-DD/data.parquet
```

- Partición Hive-style → `read_parquet('…/**/*.parquet', hive_partitioning=true)`
  con partition pruning por `tenant_id` y `snapshot_date`.
- Un solo lake para todos los clientes → métricas cross-tenant sin joins
  externos.
- Cada corrida es un **snapshot completo** e **idempotente** por tenant
  (re-correr el mismo día reemplaza el snapshot de ese tenant; escritura a
  tmp + rename atómico).
- Un tenant que falla (data.db ausente, colección rota, etc.) no aborta la
  corrida completa — se loggea y se sigue con el resto.

## Instalar

```bash
npm install
cp .env.example .env   # completar PB_WS_ADMIN_EMAIL/PASSWORD
```

Requiere Node ≥ 22.13 (`node:sqlite`). La primera corrida hace `INSTALL sqlite`
de DuckDB (descarga la extensión una vez; necesita red).

## Correr

Multi-tenant (producción — loop sobre `workspaces`):

```bash
npm run export
```

Single-tenant manual (dev/test contra una instancia `pb_*` local, salta `pb_ws`):

```bash
PB_DATA=../pb/pb_tsp/pb_data/data.db TENANT_ID=tsp npm run export
```

Validar un tenant puntual (corre su export, compara conteos raw vs. SQLite,
imprime `evm_daily` de un proyecto y lo compara contra el hook `/pm2pro/{id}`
en vivo de ese tenant):

```bash
PB_DATA=../pb/pb_tsp/pb_data/data.db TENANT_ID=tsp PB_URL=http://localhost:8092 npm run validate
```

## Config (env)

| Var | Default | Uso |
|---|---|---|
| `PB_WS_URL` | `http://localhost:8190` | PocketBase de control (workspaces) |
| `PB_WS_ADMIN_EMAIL` / `PB_WS_ADMIN_PASSWORD` | — | auth superuser contra pb_ws |
| `LAKE_DIR` | `/data/lake` | raíz del lake |
| `SNAPSHOT_DATE` | hoy (UTC) | override para backfill |
| `PB_DATA` + `TENANT_ID` | — | fuerza modo single-tenant, salta pb_ws |
| `PB_URL` (validate) | `http://localhost:8090` | hook /pm2pro del tenant a comparar |
| `VALIDATE_PROJECT_CODE` (validate) | proyecto con más reviews | proyecto piloto a inspeccionar |

## Scheduling (producción)

Diario 03:30 (después de que cada instancia PocketBase corra su cron
`sync_holidays` de las 03:00):

```bash
pm2 start bin/export.mjs --name lake-export --cron "30 3 * * *" --no-autorestart
pm2 save
```

## Colecciones exportadas (raw)

Núcleo operativo/avance, horas y recursos, financiero, dispatch — ver
`RAW_COLLECTIONS` en `src/export-tenant.mjs`. Excluidas por diseño: `users`
(PII), `config`, `pdf_templates`, `roles`, `role_permissions`, `workflows`,
y toda colección/vista `test*` / `*_duplicate` / `v_b`.

Vistas SQL (`list_hours`, `list_document_forecast`, `list_resume_hours`) se
exportan a `raw/` si la extensión sqlite de DuckDB puede leerlas; si no,
quedan documentadas en `view-definitions/{tenant}.sql` (generado en cada
corrida) para que la capa curada las reimplemente en SQL DuckDB.

## evm_daily / evm_series

Lógica portada 1:1 de `pb_hooks/get.pm2pro.pb.js` de cada tenant (misma
versión de `approcket` en todos los clientes) — mismo SQL (vía `node:sqlite`,
mismo dialecto que el hook) y mismos KPIs con sus guardas:

- `pv_pct`: curva planificada acumulada interpolada linealmente a la fecha de
  snapshot (pesos × split de credit rule: `AB0` → A .70/B .15/0 .15; default
  → A .70/0 .30).
- `ev_pct`: último acumulado de `weight × real_credit` (document_review).
- `ev_activity_pct`: ídem ponderado por horas (`hs_total/hours_oc × real_credit`).
- `hours_actual`: horas cargadas a la fecha, excluyendo `specialties.ignored_count=1`.
- `hpi = (pv_pct/100 × hours_oc) / hours_actual` — **es PV/AC, no EV/AC**; se
  porta tal cual del hook para que la validación cierre.
- `spi = ev_pct / pv_pct`.

`evm_series`: series semanales acumuladas (`planned` / `real` / `activity`)
por proyecto — insumo directo de las curvas del dashboard sin recalcular.

> TODO (fase posterior, NO implementado): `evm_daily` monetario. El único
> hook monetario (`/pxqcashspeciality`) tiene un bug conocido: el join a
> `costs_items` no filtra por período vigente (`costs.from/to`, `costs.active`).
> Al monetizar, elegir el período de costos vigente a la fecha del snapshot —
> no portar el bug. `/pxq` (a secas) es un stub roto: ignorar.

## Queries de ejemplo (DuckDB CLI)

Curva EVM histórica de un proyecto, cruzando snapshots:

```sql
SELECT snapshot_date, pv_pct, ev_pct, hpi, spi
FROM read_parquet('/data/lake/curated/evm_daily/**/*.parquet', hive_partitioning=true)
WHERE tenant_id = 'tsp' AND project_code = '<code>'
ORDER BY snapshot_date;
```

Portfolio cross-tenant del último snapshot de cada uno:

```sql
SELECT tenant_id, project_code, pv_pct, ev_pct, hpi, spi
FROM read_parquet('/data/lake/curated/evm_daily/**/*.parquet', hive_partitioning=true) e
WHERE snapshot_date = (
  SELECT max(snapshot_date) FROM read_parquet('/data/lake/curated/evm_daily/**/*.parquet', hive_partitioning=true) e2
  WHERE e2.tenant_id = e.tenant_id
)
ORDER BY tenant_id, project_code;
```

## Origen

Portado desde `approcket` rama `feature/data-lake-export`
(`scripts/lake/export.mjs`, validado contra el proyecto piloto Cashiriari
/ WHCP2029). Ese pipeline single-tenant queda como referencia histórica en
ese repo; el desarrollo activo del exporter sigue acá.
