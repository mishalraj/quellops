#!/usr/bin/env bash
set -euo pipefail

NS="${AIDBG_DEMO_NS:-aidbg-demo}"

echo "resetting aidbg-demo mesh to baseline..."

kubectl -n "$NS" scale deploy/catalog --replicas=1 >/dev/null 2>&1 || true
kubectl -n "$NS" rollout status deploy/catalog --timeout=3m >/dev/null 2>&1 || true

for dep in catalog inventory checkout edge; do
  kubectl -n "$NS" set env "deploy/${dep}" FAULT_LATENCY_MS=0 FAULT_ERROR_RATE=0 FAULT_DB_SLEEP_MS=0 >/dev/null 2>&1 || true
done

if kubectl -n "$NS" get deploy inventory >/dev/null 2>&1; then
  kubectl -n "$NS" exec deploy/inventory -- env MIGRATION_ACTION=restore_inventory_sku_index SKIP_DB_MIGRATE=1 \
    python -m services.common.migrate >/dev/null 2>&1 || true
fi

# Avoid mass rollout restart here — it creates recent_deploy noise for traffic_spike.
# Scenarios that need a fresh pod call rollout restart themselves after set env.

kubectl -n "$NS" delete job aidbg-demo-loadgen --ignore-not-found >/dev/null 2>&1 || true

echo "demo reset complete (fault env cleared; catalog replicas=1)"
