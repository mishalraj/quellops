# infra-project-poc-mishalraj

Local-first MVP for an AI production debugger:

- `agent/`: Go Kubernetes watcher agent (Deployments/Pods/Events -> backend)
- `backend/`: Python OTLP ingest, graph/anomaly/correlation APIs, optional Azure OpenAI explanation
- `deploy/`: manifests for collector, agent, and OTel demo values
- `scripts/`: local bootstrap/deploy and scenario runners
- `scenarios/`: incident scenario specs

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
- `GET /stats/services`
- `GET /graph/snapshot`
- `GET /anomalies/latest`
- `GET /incident/latest`
- `GET /explain/latest`
- `GET /k8s/events`
- `GET /query/why_slow?service=<service>`
- `GET /query/evidence?service=<service>`

## Scenario runs

- Single: `scripts/run_scenario.sh deploy_latency`
- Full suite: `scripts/run_all_scenarios.sh`

Artifacts are saved under `artifacts/<scenario>/`:

- `incident.json`
- `llm_answer.json`
- `graph_snapshot.json`
- `anomalies.json`

Schema references for deterministic artifacts are in `backend/schemas/`.

## Azure OpenAI explanation layer

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

