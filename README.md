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
imprime `evm_daily` de un proyecto y compara hpi/spi/hours_oc de **todos** los
proyectos contra el hook `/pm2pro/{id}` en vivo de ese tenant; el hook exige
login, por eso las credenciales de superuser):

```bash
PB_DATA=../pb/pb_tsp/pb_data/data.db TENANT_ID=tsp PB_URL=http://localhost:8092 \
  PB_ADMIN_EMAIL=... PB_ADMIN_PASSWORD=... npm run validate
```

## Config (env)

| Var | Default | Uso |
|---|---|---|
| `PB_WS_URL` | `http://localhost:8190` | PocketBase de control (workspaces) |
| `PB_WS_ADMIN_EMAIL` / `PB_WS_ADMIN_PASSWORD` | — | auth superuser contra pb_ws |
| `LAKE_DIR` | `/data/lake` | raíz del lake |
| `SNAPSHOT_DATE` | hoy (UTC) | override para backfill |
| `PB_DATA` + `TENANT_ID` | — | fuerza modo single-tenant, salta pb_ws |
| `EVM_LIB_PATH` | `../approcket/pocketbase/pb_hooks/lib/evm.js` | módulo EVM compartido con los hooks (ver "evm_daily / evm_series") |
| `PG_HOST` | — | si está seteado, `npm run daily` corre también `sync-postgres` |
| `PB_URL` (validate) | `http://localhost:8090` | hook /pm2pro del tenant a comparar |
| `PB_ADMIN_EMAIL` / `PB_ADMIN_PASSWORD` (validate) | — | superuser del tenant, para llamar al hook |
| `VALIDATE_PROJECT_CODE` (validate) | proyecto con más reviews | proyecto piloto a inspeccionar |

## Metabase: materializado en Postgres (opcional)

> BI externa descartada por ahora: el análisis del cliente va por MCP. Esta
> sección y `sync-postgres` quedan como referencia; `npm run daily` solo
> corre `sync-postgres` si `PG_HOST` está seteado.

**El catálogo `.duckdb` (sección siguiente) NO es viable como conexión de
Metabase en esta VPS**: el driver comunitario de DuckDB para Metabase
requiere su librería nativa (compilada contra glibc); la imagen oficial de
Metabase es Alpine (musl) y el binario nativo crashea con `SIGSEGV` al
inicializar — probado, incluso instalando `gcompat` + `libstdc++` en el
container. No se puede resolver sin reconstruir la imagen Docker de Metabase
sobre una base glibc (Debian/Ubuntu), que es demasiado para este alcance.

**Solución que sí funciona**: `bin/sync-postgres.mjs` usa la extensión
`postgres` de DuckDB (`ATTACH ... TYPE postgres`) para escribir el lake
directo a una base Postgres — el mismo Postgres que ya corre en
`metabase/docker-compose.yml` (container `metabase-postgres`, ahí también
vive la app-db de Metabase). Se creó una base separada `lake` (no reusar la
base `metabase`, es la app-db interna). Metabase se conecta con su driver
Postgres nativo — JDBC puro, sin librería nativa, cero riesgo del problema
de arriba.

```bash
# requiere el puerto 5432 expuesto en docker-compose (127.0.0.1:5432:5432)
PG_HOST=127.0.0.1 PG_PORT=5432 PG_DATABASE=lake PG_USER=metabase PG_PASSWORD=metabase \
  LAKE_DIR=/data/lake npm run sync-postgres
```

Escribe una tabla por dataset (`CREATE OR REPLACE TABLE`, reescribe entera
cada corrida — los datos son chicos):

| Tablas | Contenido |
|---|---|
| `{collection}` (una por colección raw) | solo el `snapshot_date` más reciente **de cada tenant** (estado actual — no arrastra el historial completo a Postgres, eso vive en el lake Parquet) |
| `evm_daily`, `evm_series` | histórico completo (chicas, para curvas de tendencia) |

Correr después de cada `export` — encadenado vía `npm run daily`
(`bin/daily.mjs`, ver Scheduling). Conectar Metabase: Admin → Databases →
Add → Postgres, host `postgres` (nombre del service en la red de Docker,
**no** `127.0.0.1` — eso es solo para conectar desde el host), puerto
`5432`, database `lake`.

## Catálogo DuckDB (análisis ad-hoc, CLI — no Metabase)

No usar esta vía para conectar Metabase (ver bloqueo arriba). Sirve para
análisis ad-hoc con el `duckdb` CLI o scripts Node, sin conocer la
estructura de particiones Hive del lake. `bin/build-catalog.mjs` genera un
archivo `.duckdb` persistido con vistas ya armadas:

```bash
LAKE_DIR=/data/lake CATALOG_PATH=/data/lake/lake.duckdb npm run build-catalog
```

Por cada dataset (colecciones raw, vistas raw, curated) crea dos vistas:

| Vista | Contenido |
|---|---|
| `raw_{collection}` / `curated_{name}` | histórico completo, todos los snapshots y tenants |
| `raw_{collection}_latest` / `curated_{name}_latest` | solo el `snapshot_date` más reciente **de cada tenant** (no un max global — un tenant atrasado no debe opacar a los demás) |

Para indicadores de estado actual (portfolio, KPIs) usar las `_latest`; para
curvas históricas (EVM, tendencias), las históricas.

**No hace falta reconstruir el catálogo en cada export diario** — las vistas
usan `read_parquet(glob, hive_partitioning=true)`, que resuelve el glob en
cada query. Solo hay que re-correr `build-catalog` cuando se agrega o saca un
dataset (nueva colección raw, nueva vista curada).

**Paths**: cada vista tiene embebido el `LAKE_DIR` usado al construir el
catálogo. Quien abra el `.duckdb` después (Metabase corriendo en Docker, el
`duckdb` CLI en el host) tiene que ver el lake en ese **mismo path
absoluto** — la convención acá es montar el lake en el container Docker de
Metabase en el mismo path que tiene en el host, para no traducir nada
(ver `metabase/docker-compose.yml`: monta `LAKE_DIR` 1:1 y `data/duckdb/`
donde vive `lake.duckdb`, con el driver DuckDB de Metabase ya instalado en
`plugins/`). Conectar Metabase apuntando el driver DuckDB al archivo
`.duckdb` dentro del container.

## Scheduling (producción)

Diario 03:30 (después de que cada instancia PocketBase corra su cron
`sync_holidays` de las 03:00). `bin/daily.mjs` corre `export.mjs` y, solo si
`PG_HOST` está seteado, `sync-postgres.mjs` — si `export` falla parcialmente
(algún tenant), igual sigue con `sync-postgres` para reflejar los datos
parciales; el exit code final es no-cero si cualquiera de los dos falló:

```bash
pm2 start bin/daily.mjs --name lake-daily --cron "30 3 * * *" --no-autorestart
pm2 save
```

Requiere las env vars de `export.mjs` (`LAKE_DIR`, `EVM_LIB_PATH`, `PB_WS_URL`,
`PB_WS_ADMIN_EMAIL`/`PASSWORD` o `PB_DATA`+`TENANT_ID`) y, si se usa, las de
`sync-postgres.mjs` (`PG_HOST`/`PORT`/`DATABASE`/`USER`/`PASSWORD`) en el
entorno del proceso pm2 (`pm2 start` hereda el shell actual, o usar un
ecosystem file con `env:`).

## Colecciones exportadas (raw)

Núcleo operativo/avance, horas y recursos, financiero, dispatch — ver
`RAW_COLLECTIONS` en `src/export-tenant.mjs`. Excluidas por diseño: `users`
(PII), `config`, `pdf_templates`, `roles`, `role_permissions`, `workflows`,
y toda colección/vista `test*` / `*_duplicate` / `v_b`.

`origen_type` se exporta para poder filtrar documentos cancelados
(`ignored_count = 1`), que la app deja fuera de los informes.

Vistas SQL (`list_hours`, `list_document_forecast`, `list_resume_hours`) se
exportan a `raw/` leyéndolas con `node:sqlite` (la extensión sqlite de DuckDB
no parsea sus backticks). `list_document_forecast` trae `erev_a/b/0`: fecha
de emisión de cada revisión según crédito acumulado — el criterio de
"emitida / pendiente" de la app. Las definiciones quedan además en
`view-definitions/{tenant}.sql` (generado en cada corrida).

## evm_daily / evm_series

**La lógica EVM no está copiada en este repo.** El exporter carga el mismo
módulo que usa el hook `/pm2pro` de la app
(`approcket/pocketbase/pb_hooks/lib/evm.js`, vía `src/evm-lib.mjs` y
`EVM_LIB_PATH`): mismas queries y misma matemática de KPIs, así que un cambio
en el hook llega al lake sin tocar nada acá. En producción apuntar
`EVM_LIB_PATH` al `pb_hooks/lib/evm.js` con el que corren las instancias
PocketBase. Requiere que el tenant tenga las migraciones de approcket al día
(ej. `origen_type.ignored_count`); si no, ese tenant falla con
`no such column` y el resto sigue.

- `pct_a` / `pct_b` / `pct_0`: split de la credit rule del proyecto
  (default .70 / 0 / .30).
- `hours_oc`: horas presupuestadas, sin documentos cancelados.
- `pv_pct`: curva planificada acumulada interpolada linealmente a la fecha de
  snapshot (pesos × split de la credit rule).
- `ev_pct`: último acumulado de `weight × real_credit` (document_review).
- `ev_activity_pct`: ídem ponderado por horas (`hs_total/hours_oc × real_credit`),
  sin documentos cancelados.
- `hours_actual`: horas cargadas a la fecha, excluyendo `specialties.ignored_count=1`.
- `hpi = (pv_pct/100 × hours_oc) / hours_actual` — **es PV/AC, no EV/AC**; así
  lo define el hook.
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
