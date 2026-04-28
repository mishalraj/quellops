#!/usr/bin/env bash
set -euo pipefail

# For real use, build and push image then update deploy/agent/agent.yaml.
echo "NOTE: deploy/agent/agent.yaml references placeholder image."
kubectl apply -f deploy/agent/agent.yaml

