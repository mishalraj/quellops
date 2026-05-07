# Background Start + Scenario Runbook

This runbook starts all required services in the background, then runs scenarios reliably.

## 1) One-time prerequisites

- `kind`, `kubectl`, `helm`, `docker`, `python3`, `curl`
- Run all commands from repo root:

```bash
cd ~/infra-project-poc-mishalraj
```

## 2) Start cluster + demo + collector + agent

Use the project bootstrap script (it is idempotent for existing kind clusters):

```bash
scripts/bootstrap_local.sh
```

This runs:
- `scripts/setup_kind.sh`
- `scripts/deploy_otel_demo.sh`
- `scripts/deploy_collector.sh`
- `scripts/deploy_agent.sh`

## 3) Start backend in background

Use Docker mode and keep it detached:

```bash
BACKEND_MODE=docker docker compose up -d --build aidbg-backend
```

Health check:

```bash
curl -fsS http://localhost:8088/healthz
```

Expected: `{"ok":true}`

## 4) Verify ingest path before scenarios

Wait 20-60s, then confirm traces are flowing:

```bash
curl -fsS http://localhost:8088/stats/ingest
```

You should see `spans_received > 0`.

If spans stay at 0, check collector logs:

```bash
kubectl -n aidbg-otel logs deploy/aidbg-otel-collector --tail=200
```

### Linux VM fix for `host.docker.internal` (important)

If logs show:

- `lookup host.docker.internal ... no such host`

then the collector cannot resolve Docker Desktop hostname (common on Linux VMs).

Set exporter endpoint to your Docker bridge gateway (commonly `172.18.0.1:4317`):

```bash
kubectl -n aidbg-otel set env deploy/aidbg-otel-collector AIDBG_BACKEND_OTLP_ENDPOINT=172.18.0.1:4317
kubectl -n aidbg-otel rollout status deploy/aidbg-otel-collector --timeout=3m
```

Then verify:

```bash
kubectl -n aidbg-otel logs deploy/aidbg-otel-collector --tail=100 | grep -E "Everything is ready|Exporting failed|no such host" || true
curl -fsS http://localhost:8088/stats/ingest
```

If `172.18.0.1` is not reachable in your environment, use your host-reachable bridge IP and repeat the same `set env` command.

## 5) Kafka issue: what happened and how to handle it

### Symptom we saw

Errors like:
- `nc: bad address 'kafka'`
- OTel demo rollouts hanging because components expected Kafka.

### Why

`deploy/otel-demo/values-light.yaml` disables Kafka + accounting by default:

- `components.kafka.enabled: false`
- `components.accounting.enabled: false`

If other chart changes or older workloads still reference Kafka, you can get the `kafka` DNS/address errors.

### Recommended fix path

Choose one consistent mode and keep it stable during scenario runs:

1. **Light mode (default, lower resources):** keep Kafka/accounting disabled.
2. **Kafka mode (if your environment expects it):** enable Kafka (and accounting if needed), then redeploy.

If you need Kafka mode, edit `deploy/otel-demo/values-light.yaml`:

```yaml
components:
  kafka:
    enabled: true
  accounting:
    enabled: true
```

Then redeploy:

```bash
scripts/deploy_otel_demo.sh
kubectl -n otel-demo rollout status deployment --timeout=10m || true
```

After redeploy, re-check:

```bash
kubectl -n otel-demo get pods
curl -fsS http://localhost:8088/stats/ingest
```

## 6) Warm traffic in background (recommended)

This helps baseline/anomaly windows before scenario assertions:

```bash
nohup env DURATION_SECONDS=180 RPS=8 scripts/run_load.sh > /tmp/aidbg-load.log 2>&1 &
```

## 7) Run scenarios

Run full suite:

```bash
scripts/run_all_scenarios.sh
```

Or run one scenario:

```bash
scripts/run_scenario.sh deploy_latency
scripts/run_scenario.sh db_slowdown
scripts/run_scenario.sh traffic_spike_false_positive
scripts/run_scenario.sh dependency_down
```

Artifacts are saved under `artifacts/<scenario>/`.

## 8) Useful background operations

Follow backend logs:

```bash
docker compose logs -f aidbg-backend
```

Restart backend:

```bash
docker compose restart aidbg-backend
```

Stop everything:

```bash
docker compose down
kind delete cluster --name aidbg-local
```

## 9) Quick troubleshooting checklist

- `curl http://localhost:8088/healthz` fails -> backend not up.
- `spans_received` not increasing -> collector/export path issue.
- `recent_deploy` dominates unexpectedly -> let rollout settle, warm traffic, rerun scenario.
- `dependency_down` not ranking first -> ensure `cart` is actually scaled to 0 and fresh events are present.

