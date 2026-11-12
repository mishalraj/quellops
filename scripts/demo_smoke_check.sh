#!/usr/bin/env bash
# Read-only checks after deploy — run on VM after start_stack_bg_and_check.sh
set -euo pipefail

BACKEND_URL="${BACKEND_URL:-http://localhost:8088}"
NS="${AIDBG_DEMO_NS:-quellops-demo}"
ADMIN_TOKEN="${AIDBG_ADMIN_API_TOKEN:-}"
if [[ -z "$ADMIN_TOKEN" && -f .env ]]; then ADMIN_TOKEN="$(sed -n 's/^AIDBG_ADMIN_API_TOKEN=//p' .env | tail -n 1)"; fi

echo "=== quellops-demo smoke check ==="
kubectl -n "$NS" get pods
curl -fsS "${BACKEND_URL}/healthz" && echo ""
curl -fsS -H "Authorization: Bearer ${ADMIN_TOKEN:?AIDBG_ADMIN_API_TOKEN is required}" "${BACKEND_URL}/stats/ingest" && echo ""
echo ""
echo "Next: kubectl -n $NS port-forward svc/edge 8080:8080"
echo "Then: EDGE_URL=http://localhost:8080/ python3 demo/loadgen/load.py"
echo "Then: scripts/run_demo_scenario.sh 01_deploy_latency"
