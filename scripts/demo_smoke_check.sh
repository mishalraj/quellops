#!/usr/bin/env bash
# Read-only checks after deploy — run on VM after start_stack_bg_and_check.sh
set -euo pipefail

BACKEND_URL="${BACKEND_URL:-http://localhost:8088}"
NS="${AIDBG_DEMO_NS:-quellops-demo}"

echo "=== quellops-demo smoke check ==="
kubectl -n "$NS" get pods
curl -fsS "${BACKEND_URL}/healthz" && echo ""
curl -fsS "${BACKEND_URL}/stats/ingest" && echo ""
echo ""
echo "Next: kubectl -n $NS port-forward svc/edge 8080:8080"
echo "Then: EDGE_URL=http://localhost:8080/ python3 demo/loadgen/load.py"
echo "Then: scripts/run_demo_scenario.sh 01_deploy_latency"
