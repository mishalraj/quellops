#!/usr/bin/env bash
set -euo pipefail

CLUSTER_NAME="${CLUSTER_NAME:-aidbg-local}"
AGENT_IMAGE="${AGENT_IMAGE:-aidbg-agent:dev}"
AGENT_BACKEND_URL="${AIDBG_BACKEND_URL:-http://host.docker.internal:8088}"
AGENT_CLUSTER_NAME="${AIDBG_CLUSTER_NAME:-$CLUSTER_NAME}"

echo "building agent image: $AGENT_IMAGE"
docker build -t "$AGENT_IMAGE" ./agent

echo "loading image into kind cluster: $CLUSTER_NAME"
kind load docker-image "$AGENT_IMAGE" --name "$CLUSTER_NAME"

scripts/install_k8s_agent.sh \
  --backend-url "$AGENT_BACKEND_URL" \
  --cluster "$AGENT_CLUSTER_NAME" \
  --image "$AGENT_IMAGE"
echo "agent deployed"
