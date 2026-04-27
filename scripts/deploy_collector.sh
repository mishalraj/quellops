#!/usr/bin/env bash
set -euo pipefail

kubectl apply -f deploy/otel-collector/collector.yaml
kubectl -n aidbg-otel rollout status deployment/aidbg-otel-collector --timeout=5m
echo "collector ready"

