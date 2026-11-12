#!/usr/bin/env bash
set -euo pipefail

CLUSTER_NAME="${CLUSTER_NAME:-quellops-local}"
DEMO_DIR="${DEMO_DIR:-demo}"
BACKEND_HOST="${BACKEND_HOST:-172.18.0.1}"
SERVICES=(catalog inventory checkout edge)

echo "building demo service images..."
for svc in "${SERVICES[@]}"; do
  docker build \
    --build-arg SERVICE="${svc}" \
    -t "quellops-demo/${svc}:dev" \
    -f "${DEMO_DIR}/Dockerfile.service" \
    "${DEMO_DIR}"
  kind load docker-image "quellops-demo/${svc}:dev" --name "${CLUSTER_NAME}"
done

docker build -t quellops-demo/loadgen:dev -f "${DEMO_DIR}/Dockerfile.loadgen" "${DEMO_DIR}"
kind load docker-image quellops-demo/loadgen:dev --name "${CLUSTER_NAME}"

echo "applying quellops-demo manifests..."
kubectl apply -f deploy/quellops-demo/namespace.yaml
CHANGE_INGEST_TOKEN="${AIDBG_CHANGE_INGEST_TOKEN:-}"
if [[ -z "$CHANGE_INGEST_TOKEN" && -f .env ]]; then
  CHANGE_INGEST_TOKEN="$(sed -n 's/^AIDBG_CHANGE_INGEST_TOKEN=//p' .env | tail -n 1)"
fi
if [[ -z "$CHANGE_INGEST_TOKEN" ]]; then
  echo "AIDBG_CHANGE_INGEST_TOKEN is required (set it or run ./install.sh first)" >&2
  exit 1
fi
kubectl -n quellops-demo create secret generic quellops-api-tokens \
  --from-literal=change-ingest-token="$CHANGE_INGEST_TOKEN" \
  --dry-run=client -o yaml | kubectl apply -f -
kubectl apply -f deploy/quellops-demo/postgres.yaml
kubectl -n quellops-demo rollout status deploy/postgres --timeout=5m
kubectl apply -f deploy/quellops-demo/catalog.yaml
kubectl apply -f deploy/quellops-demo/inventory.yaml
kubectl apply -f deploy/quellops-demo/checkout.yaml
kubectl apply -f deploy/quellops-demo/edge.yaml
kubectl apply -f deploy/quellops-demo/pg-stat-scraper.yaml

kubectl -n quellops-demo rollout status deploy/catalog --timeout=5m
kubectl -n quellops-demo rollout status deploy/inventory --timeout=5m
kubectl -n quellops-demo rollout status deploy/checkout --timeout=5m
kubectl -n quellops-demo rollout status deploy/edge --timeout=5m

# Linux VM: agent must reach host backend (same as collector OTLP endpoint host).
if kubectl -n quellops-agent get deploy quellops-agent >/dev/null 2>&1; then
  kubectl -n quellops-agent set env deploy/quellops-agent \
    "AIDBG_BACKEND_URL=http://${BACKEND_HOST}:8088" >/dev/null || true
  kubectl -n quellops-agent rollout status deploy/quellops-agent --timeout=3m >/dev/null || true
fi

echo "quellops-demo mesh ready (namespace quellops-demo)"
echo "  port-forward: kubectl -n quellops-demo port-forward svc/edge 8080:8080"
echo "  load in-cluster: kubectl apply -f deploy/quellops-demo/loadgen-job.yaml"
