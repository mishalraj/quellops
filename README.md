# infra-project-poc-mishalraj

Local-first MVP for an AI production debugger:

- `agent/`: Go Kubernetes watcher agent (Deployments/Pods/Events -> backend)
- `backend/`: Python OTLP ingest, graph/anomaly/correlation APIs, optional Azure OpenAI explanation
- `demo/`: four-service Python mesh (edge → checkout → inventory → catalog) for POC tests
- `deploy/`: manifests for collector, agent, and aidbg-demo mesh
- `scripts/`: local bootstrap/deploy and scenario runners
- `scenarios/`: incident scenario specs (legacy otel-demo) and `scenarios/demo/` for the small mesh

## Quick start

1. `scripts/bootstrap_local.sh`
2. Start backend:
   - Docker mode: `BACKEND_MODE=docker scripts/backend_up.sh`
   - Venv mode: `scripts/backend_up.sh`
3. **Demo UI:** open [http://localhost:8088/ui/](http://localhost:8088/ui/) (live ingest, top causes, anomalies, explain summary). Root [http://localhost:8088/](http://localhost:8088/) redirects there.

## Backend APIs (MVP)

- `GET /healthz`
- `GET /stats/ingest`
- `POST /ingest/k8s`
- `POST /ingest/change` — normalized change events (`code.push`, `config.changed`, `db.migration.applied`, `db.slow_query_stat`)
- `POST /webhook/github` — GitHub push webhook → `code.push` (uses `backend/service_map.yaml`)
- `GET /changes/recent`
- `GET /stats/services`
- `GET /graph/snapshot`
- `GET /anomalies/latest`
- `GET /incident/latest`
- `GET /explain/latest`
- `GET /k8s/events`
- `GET /query/why_slow?service=<service>`
- `GET /query/evidence?service=<service>`

## Demo mesh (recommended for POC)

1. `scripts/bootstrap_local.sh` (Kind + collector + agent + **aidbg-demo** mesh)
2. `BACKEND_MODE=docker docker compose up -d --build aidbg-backend`
3. Port-forward edge: `kubectl -n aidbg-demo port-forward svc/edge 8080:8080`
4. Baseline load: `EDGE_URL=http://localhost:8080/ DURATION_SECONDS=60 RPS=5 python3 demo/loadgen/load.py`
5. Run one scenario at a time:
   - `scripts/demo_reset.sh`
   - `scripts/run_demo_scenario.sh 01_deploy_latency`
   - (repeat reset + scenario for `02_dependency_down`, `03_traffic_spike`, `04_upstream_errors`, `05_db_slowdown`)

Or use `scripts/start_stack_bg_and_check.sh` for full stack + health summary.

Full demo suite (reset between each): `scripts/run_all_demo_scenarios.sh`

## Legacy otel-demo scenarios

- Single: `scripts/run_scenario.sh deploy_latency`
- Full suite: `scripts/run_all_scenarios.sh`

Artifacts are saved under `artifacts/<scenario>/` or `artifacts/demo/<scenario>/`:

- `incident.json`
- `llm_answer.json`
- `graph_snapshot.json`
- `anomalies.json`

Schema references for deterministic artifacts are in `backend/schemas/`.

## Azure OpenAI explanation layer

Step-by-step: [docs/LLM_SETUP.md](docs/LLM_SETUP.md).

Set these env vars before launching backend:

- `AZURE_OPENAI_ENDPOINT`
- `AZURE_OPENAI_API_KEY`
- `AZURE_OPENAI_API_VERSION` (default `2024-12-01-preview`)
- `AZURE_OPENAI_DEPLOYMENT` (for example `gpt-4.1-mini`)

If missing, `/explain/latest` returns deterministic fallback JSON instead of failing.

## Auto-instrumentation webhook (post-MVP scaffold)

- Apply with: `scripts/deploy_webhook.sh`
- Label namespace for opt-in: `kubectl label ns <ns> aidbg-instrument=true`
- Label workload pods with `aidbg-instrument=true` to request injection.

