# aidbg demo mesh (4 services + Postgres)

Minimal Python FastAPI chain for POC testing:

`edge` → `checkout` → `inventory` → `catalog`

`inventory` uses **Postgres** (OTel `psycopg2` spans) for stock queries.

Deploy with `scripts/deploy_quellops_demo.sh`. Run scenarios with `scripts/run_demo_scenario.sh <name>`.

## Fault injection

| Env | Effect |
|-----|--------|
| `FAULT_LATENCY_MS` | Sleep before handling request |
| `FAULT_ERROR_RATE` | Random 500 responses |
| `FAULT_DB_SLEEP_MS` | `pg_sleep` on DB queries (slow query demo) |

## DB migrations & change events

- Schema bootstrap: `python -m services.common.migrate` (`MIGRATION_ACTION=ensure_schema`)
- Apply named migration: `MIGRATION_ACTION=drop_inventory_sku_index python -m services.common.migrate`
- Migrations POST `db.migration.applied` to `AIDBG_CHANGE_INGEST_URL` when set.

## pg_stat_statements scraper

CronJob `deploy/quellops-demo/pg-stat-scraper.yaml` posts cluster-wide slow query stats to `/ingest/change`.

## Scenarios

Including `05_db_slowdown`: drops index, injects DB sleep, simulates `code.push`, expects `db_slow_query` / `db_migration` in top causes.
