#!/usr/bin/env bash
set -euo pipefail

kubectl apply -f deploy/otel-collector/collector.yaml
kubectl -n quellops-otel rollout status deployment/quellops-otel-collector --timeout=5m
echo "collector ready"

