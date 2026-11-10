# Delivery Status and Pending Breakdown

**Program:** `aidbg` local-first POC
**Last updated:** 2026-06-01

---

## 1) Workstream summary (clean view)

| Workstream | Status | Completed tasks | Pending tasks |
|---|---|---:|---:|
| Mesh services | Done | 4 | 0 |
| Kubernetes deployment | Done | 4 | 0 |
| DB plugin and DB signals | Partially done | 3 | 1 |
| Backend incident logic | Mostly done | 4 | 1 |
| Scenario framework | Mostly done | 4 | 1 |
| Local run and docs | Mostly done | 4 | 2 |
| Release readiness | Pending | 0 | 2 |
| MVP deferred backlog | Deferred | 0 | 3 |

---

## 2) Mesh services (separate)

| Task ID | Task | Status | Notes |
|---|---|---|---|
| MESH-01 | Create 4-service mesh (`edge -> checkout -> inventory -> catalog`) | Done | Implemented under `demo/services/` |
| MESH-02 | Add shared service helpers (OTel setup, app factory, faults) | Done | Implemented under `demo/services/common/` |
| MESH-03 | Add service container build path | Done | `demo/Dockerfile.service`, `demo/requirements.txt` |
| MESH-04 | Add load generator for mesh traffic | Done | `demo/loadgen/load.py`, `demo/Dockerfile.loadgen` |

---

## 3) Kubernetes deployment (separate)

| Task ID | Task | Status | Notes |
|---|---|---|---|
| K8S-01 | Create demo namespace and service manifests | Done | `deploy/aidbg-demo/namespace.yaml` + service manifests |
| K8S-02 | Add deploy script for demo stack | Done | `scripts/deploy_aidbg_demo.sh` |
| K8S-03 | Add reset and smoke-check scripts | Done | `scripts/demo_reset.sh`, `scripts/demo_smoke_check.sh` |
| K8S-04 | Add collector/agent host wiring in deployment flow | Done | Endpoint env handling included in scripts |

---

## 4) DB plugin and DB observability (separate)

| Task ID | Task | Status | Notes |
|---|---|---|---|
| DB-01 | Enable Postgres with `pg_stat_statements` preload and extension | Done | `deploy/aidbg-demo/postgres.yaml` |
| DB-02 | Add periodic scraper for `pg_stat_statements` | Done | `deploy/aidbg-demo/pg-stat-scraper.yaml`, `demo/tools/pg_stat_scraper.py` |
| DB-03 | Ingest DB stat signal in backend ranking pipeline | Done | `db.slow_query_stat` supported in `backend/src/aidbg/engine.py` |
| DB-04 | Replace Postgres-only collection with generic adapter contract | Pending | See `docs/DB_OBSERVABILITY_GENERIC_WORK_PACKAGE.md` |
| DB-05 | Add production safety controls (timeout, rate limit, backoff, kill switch) | Pending | Must be complete before prod usage |
| DB-06 | Implement adaptive/sampled collection strategy (avoid continuous heavy polling) | Pending | Current schedule-based approach is not sufficient for prod |
| DB-07 | Add generic DB spike/change signal schema and backend mapping | Pending | Extend beyond `db.slow_query_stat` only |
| DB-08 | Validate staging impact and document safe rollout defaults | Pending | Include read/write impact notes and guardrails |

---

## 5) Backend incident logic (separate)

| Task ID | Task | Status | Notes |
|---|---|---|---|
| BE-01 | Remove misleading confidence KPI path | Done | Backend/UI field cleanup completed |
| BE-02 | Improve ranking logic for deploy/dependency/traffic scenarios | Done | Scoring and ordering updated in `engine.py` |
| BE-03 | Add deploy-latency tie-break behavior | Done | `01_deploy_latency` fixed and validated |
| BE-04 | Add trace focus snapshot for UI explainability | Done | `trace_focus_snapshot()` and `/trace/focus` flow |
| BE-05 | Add unit tests for ranking regressions | Pending | Tests not yet added for ordering/tie behavior |

---

## 6) Scenario framework (separate)

| Task ID | Task | Status | Notes |
|---|---|---|---|
| SCN-01 | Create demo scenarios 01-05 | Done | `scenarios/demo/*.yaml` |
| SCN-02 | Add scenario runner with artifact capture and assertions | Done | `scripts/run_demo_scenario.sh` |
| SCN-03 | Add full suite runner with reset between scenarios | Done | `scripts/run_all_demo_scenarios.sh` |
| SCN-04 | Validate `01_deploy_latency` after ranking fix | Done | Verified pass |
| SCN-05 | Revalidate complete suite (`01..05`) on latest branch | Pending | Full green run still pending |

---

## 7) Local run and docs (separate)

| Task ID | Task | Status | Notes |
|---|---|---|---|
| OPS-01 | Add one-command local runner | Done | `scripts/run_local.sh` |
| OPS-02 | Add backend readiness wait in local runner | Done | Startup race condition fixed |
| OPS-03 | Update runbook for demo mesh flow | Done | `docs/BACKGROUND_START_AND_SCENARIO_RUNBOOK.md` |
| OPS-04 | Add status/core-logic documentation | Done | `docs/DELIVERY_STATUS_AND_PENDING_BREAKDOWN.md`, `docs/CURRENT_OBJECTIVE_AND_CORE_LOGIC.md` |
| OPS-05 | Make README and runbook fully canonical around one startup command | Pending | Minor consistency cleanup still pending |
| OPS-06 | Add concise troubleshooting matrix section | Pending | Docker/ingest/host mapping matrix to add |

---

## 8) Release readiness (separate)

| Task ID | Task | Status | Notes |
|---|---|---|---|
| REL-01 | Clean local noise (`__pycache__`, transient artifacts) | Pending | Repo contains generated files/noise |
| REL-02 | Package changes into reviewable commit set | Pending | Work is still uncommitted |

---

## 9) Deferred MVP backlog (separate)

| Task ID | Task | Status | Notes |
|---|---|---|---|
| MVP-01 | Persist signals/incidents beyond memory | Deferred | Post-POC |
| MVP-02 | Add job orchestration for async correlation/explain | Deferred | Post-POC |
| MVP-03 | Improve correlation/ranking model sophistication | Deferred | Post-POC |
