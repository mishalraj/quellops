#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BACKEND_URL="${AIDBG_BACKEND_URL:-http://host.docker.internal:8088}"
IMAGE="${AIDBG_K8S_AGENT_IMAGE:-ghcr.io/mishalraj/quellops-kubernetes-agent:latest}"
CLUSTER_NAME="${AIDBG_CLUSTER_NAME:-}"
CLUSTER_TOKEN="${AIDBG_K8S_CLUSTER_TOKEN:-}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --backend-url) BACKEND_URL="$2"; shift 2 ;;
    --image) IMAGE="$2"; shift 2 ;;
    --cluster) CLUSTER_NAME="$2"; shift 2 ;;
    --cluster-token) CLUSTER_TOKEN="$2"; shift 2 ;;
    *) echo "Usage: $0 [--backend-url URL] [--image IMAGE] [--cluster NAME] [--cluster-token TOKEN]"; exit 1 ;;
  esac
done

command -v kubectl >/dev/null 2>&1 || { echo "kubectl is required"; exit 1; }
kubectl cluster-info >/dev/null

if [[ -z "${AIDBG_K8S_AGENT_TOKEN:-}" && -f "$ROOT_DIR/.env" ]]; then
  AIDBG_K8S_AGENT_TOKEN="$(sed -n 's/^AIDBG_K8S_AGENT_TOKEN=//p' "$ROOT_DIR/.env" | tail -n 1)"
fi
[[ -n "${AIDBG_K8S_AGENT_TOKEN:-}" ]] || {
  echo "AIDBG_K8S_AGENT_TOKEN is missing. Run ./install.sh first or export the token."
  exit 1
}
if [[ -z "$CLUSTER_NAME" ]]; then
  CLUSTER_NAME="$(kubectl config current-context)"
fi

kubectl create namespace quellops-agent --dry-run=client -o yaml | kubectl apply -f -
SECRET_ARGS=(--from-literal="token=$AIDBG_K8S_AGENT_TOKEN")
if [[ -n "$CLUSTER_TOKEN" ]]; then
  SECRET_ARGS+=(--from-literal="cluster-token=$CLUSTER_TOKEN")
fi
kubectl -n quellops-agent create secret generic quellops-agent-credentials \
  "${SECRET_ARGS[@]}" --dry-run=client -o yaml | kubectl apply -f -
kubectl apply -f "$ROOT_DIR/deploy/agent/agent.yaml"
kubectl -n quellops-agent set image deployment/quellops-agent agent="$IMAGE"
kubectl -n quellops-agent set env deployment/quellops-agent \
  AIDBG_BACKEND_URL="$BACKEND_URL" AIDBG_CLUSTER_NAME="$CLUSTER_NAME"
kubectl -n quellops-agent rollout restart deployment/quellops-agent
kubectl -n quellops-agent rollout status deployment/quellops-agent --timeout=120s

echo "Kubernetes agent installed for $CLUSTER_NAME"
echo "Open $BACKEND_URL/ui/kubernetes.html and choose the namespaces to monitor."
