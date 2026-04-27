#!/usr/bin/env bash
set -euo pipefail

NAMESPACE="${NAMESPACE:-otel-demo}"
RELEASE="${RELEASE:-otel-demo}"

echo "installing OpenTelemetry helm repo..."
helm repo add open-telemetry https://open-telemetry.github.io/opentelemetry-helm-charts >/dev/null
helm repo update >/dev/null

kubectl get ns "$NAMESPACE" >/dev/null 2>&1 || kubectl create ns "$NAMESPACE"

echo "installing OpenTelemetry Demo chart..."
helm upgrade --install "$RELEASE" open-telemetry/opentelemetry-demo \
  --namespace "$NAMESPACE" \
  -f deploy/otel-demo/values-light.yaml

kubectl -n "$NAMESPACE" rollout status deployment --timeout=10m || true
echo "otel-demo install invoked (some components may take time to become Ready)"

