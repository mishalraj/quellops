#!/usr/bin/env bash
set -euo pipefail

CLUSTER_NAME="${CLUSTER_NAME:-aidbg-local}"
DEMO_DIR="${DEMO_DIR:-demo}"
BACKEND_HOST="${BACKEND_HOST:-172.18.0.1}"
SERVICES=(catalog inventory checkout edge)

echo "building demo service images..."
for svc in "${SERVICES[@]}"; do
  docker build \
    --build-arg SERVICE="${svc}" \
    -t "aidbg-demo/${svc}:dev" \
    -f "${DEMO_DIR}/Dockerfile.service" \
    "${DEMO_DIR}"
  kind load docker-image "aidbg-demo/${svc}:dev" --name "${CLUSTER_NAME}"
done

docker build -t aidbg-demo/loadgen:dev -f "${DEMO_DIR}/Dockerfile.loadgen" "${DEMO_DIR}"
kind load docker-image aidbg-demo/loadgen:dev --name "${CLUSTER_NAME}"

echo "applying aidbg-demo manifests..."
kubectl apply -f deploy/aidbg-demo/namespace.yaml
kubectl apply -f deploy/aidbg-demo/postgres.yaml
kubectl -n aidbg-demo rollout status deploy/postgres --timeout=5m
kubectl apply -f deploy/aidbg-demo/catalog.yaml
kubectl apply -f deploy/aidbg-demo/inventory.yaml
kubectl apply -f deploy/aidbg-demo/checkout.yaml
kubectl apply -f deploy/aidbg-demo/edge.yaml
kubectl apply -f deploy/aidbg-demo/pg-stat-scraper.yaml

kubectl -n aidbg-demo rollout status deploy/catalog --timeout=5m
kubectl -n aidbg-demo rollout status deploy/inventory --timeout=5m
kubectl -n aidbg-demo rollout status deploy/checkout --timeout=5m
kubectl -n aidbg-demo rollout status deploy/edge --timeout=5m

# Linux VM: agent must reach host backend (same as collector OTLP endpoint host).
if kubectl -n aidbg-agent get deploy aidbg-agent >/dev/null 2>&1; then
  kubectl -n aidbg-agent set env deploy/aidbg-agent \
    "AIDBG_BACKEND_EVENT_URL=http://${BACKEND_HOST}:8088/ingest/k8s" \
    "AIDBG_BACKEND_CHANGE_URL=http://${BACKEND_HOST}:8088/ingest/change" >/dev/null || true
  kubectl -n aidbg-agent rollout status deploy/aidbg-agent --timeout=3m >/dev/null || true
fi

echo "aidbg-demo mesh ready (namespace aidbg-demo)"
echo "  port-forward: kubectl -n aidbg-demo port-forward svc/edge 8080:8080"
echo "  load in-cluster: kubectl apply -f deploy/aidbg-demo/loadgen-job.yaml"
