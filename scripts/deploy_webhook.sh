#!/usr/bin/env bash
set -euo pipefail

echo "applying auto-instrumentation webhook (MVP scaffold)..."
kubectl apply -f deploy/auto-instrumentation/webhook.yaml
echo "webhook applied"
