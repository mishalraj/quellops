#!/usr/bin/env bash
set -euo pipefail

CLUSTER_NAME="${CLUSTER_NAME:-aidbg-local}"
AGENT_IMAGE="${AGENT_IMAGE:-aidbg-agent:dev}"

echo "building agent image: $AGENT_IMAGE"
docker build -t "$AGENT_IMAGE" ./agent

echo "loading image into kind cluster: $CLUSTER_NAME"
kind load docker-image "$AGENT_IMAGE" --name "$CLUSTER_NAME"

kubectl apply -f deploy/agent/agent.yaml
kubectl -n aidbg-agent set image deployment/aidbg-agent agent="$AGENT_IMAGE"
kubectl -n aidbg-agent rollout status deployment/aidbg-agent --timeout=5m
echo "agent deployed"

