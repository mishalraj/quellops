#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

CLUSTER_NAME="${QUELLOPS_SOAK_CLUSTER:-quellops-soak}"
CONTEXT="kind-${CLUSTER_NAME}"
NAMESPACE="quellops-soak"
AGENT_NAMESPACE="quellops-agent"
DURATION="${QUELLOPS_SOAK_DURATION_SECONDS:-180}"
KEEP_CLUSTER="${QUELLOPS_SOAK_KEEP_CLUSTER:-false}"
WORK_DIR="$(mktemp -d "${TMPDIR:-/tmp}/quellops-soak.XXXXXX")"
STATE_FILE="${WORK_DIR}/receiver-state.json"
RECEIVER_PID=""
PORT="$(python3 -c 'import socket; s=socket.socket(); s.bind(("0.0.0.0", 0)); print(s.getsockname()[1]); s.close()')"

stop_receiver() {
  if [[ -n "$RECEIVER_PID" ]]; then
    kill "$RECEIVER_PID" >/dev/null 2>&1 || true
    wait "$RECEIVER_PID" 2>/dev/null || true
    RECEIVER_PID=""
  fi
}

cleanup() {
  stop_receiver
  if [[ "$KEEP_CLUSTER" != "true" ]]; then
    kind delete cluster --name "$CLUSTER_NAME" >/dev/null 2>&1 || true
  fi
  rm -rf "$WORK_DIR"
}
trap cleanup EXIT

for command in docker kind kubectl helm curl python3; do
  command -v "$command" >/dev/null || {
    echo "missing required command: $command" >&2
    exit 1
  }
done

start_receiver() {
  python3 scripts/kubernetes_soak_receiver.py --port "$PORT" --state "$STATE_FILE" --namespace "$NAMESPACE" >"${WORK_DIR}/receiver.log" 2>&1 &
  RECEIVER_PID=$!
  for _ in $(seq 1 40); do
    if curl -fsS "http://127.0.0.1:${PORT}/healthz" >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.25
  done
  cat "${WORK_DIR}/receiver.log" >&2
  return 1
}

stats_value() {
  local key="$1"
  curl -fsS "http://127.0.0.1:${PORT}/stats" |
    python3 -c "import json,sys; print(json.load(sys.stdin).get('${key}', 0))"
}

wait_for_value() {
  local key="$1" minimum="$2" timeout="$3"
  local deadline=$((SECONDS + timeout))
  while ((SECONDS < deadline)); do
    local value
    value="$(stats_value "$key" 2>/dev/null || echo 0)"
    if ((value >= minimum)); then
      return 0
    fi
    sleep 2
  done
  echo "timed out waiting for ${key} >= ${minimum}" >&2
  curl -fsS "http://127.0.0.1:${PORT}/stats" >&2 || true
  return 1
}

if kind get clusters | grep -qx "$CLUSTER_NAME"; then
  kind delete cluster --name "$CLUSTER_NAME"
fi

cat >"${WORK_DIR}/kind.yaml" <<'YAML'
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes:
  - role: control-plane
  - role: worker
  - role: worker
YAML

echo "Creating three-node Kind cluster ${CLUSTER_NAME}"
kind create cluster --name "$CLUSTER_NAME" --config "${WORK_DIR}/kind.yaml" --wait 5m

echo "Building and loading the real QuellOps agent"
docker build -t quellops-agent:soak -f agent/Dockerfile agent >/dev/null
kind load docker-image quellops-agent:soak --name "$CLUSTER_NAME"
start_receiver

kubectl --context "$CONTEXT" create namespace "$NAMESPACE"
helm_args=(
  --kube-context "$CONTEXT"
  --namespace "$AGENT_NAMESPACE"
  --create-namespace
  --set image.repository=quellops-agent
  --set image.tag=soak
  --set image.pullPolicy=Never
  --set controlPlaneURL="http://host.docker.internal:${PORT}"
  --set clusterID=soak-cluster
  --set clusterName=soak-cluster
  --set credentials.bootstrapToken=soak-bootstrap-token
  --set 'watchNamespaces[0]=quellops-soak'
  --wait
  --timeout 5m
)
helm upgrade --install soak-agent charts/quellops-agent "${helm_args[@]}"

kubectl --context "$CONTEXT" -n "$NAMESPACE" apply -f - <<'YAML'
apiVersion: apps/v1
kind: Deployment
metadata:
  name: churn
spec:
  replicas: 1
  selector:
    matchLabels: {app: churn}
  template:
    metadata:
      labels: {app: churn}
    spec:
      containers:
        - name: churn
          image: registry.k8s.io/pause:3.10.1
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: soak-data
spec:
  accessModes: [ReadWriteOnce]
  resources:
    requests:
      storage: 16Mi
---
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: soak-ingress
spec:
  rules:
    - host: soak.local
      http:
        paths:
          - path: /
            pathType: Prefix
            backend:
              service:
                name: missing-service
                port:
                  number: 80
YAML

wait_for_value registrations 1 120
wait_for_value accepted 1 120
baseline="$(stats_value accepted)"
echo "Initial delivery confirmed with ${baseline} events"

echo "Stopping the receiver and generating events during the outage"
stop_receiver
for index in $(seq 1 12); do
  replicas=$((index % 3 + 1))
  kubectl --context "$CONTEXT" -n "$NAMESPACE" scale deployment/churn --replicas "$replicas" >/dev/null
  kubectl --context "$CONTEXT" -n "$NAMESPACE" annotate deployment/churn "soak.quellops.io/revision=${index}" --overwrite >/dev/null
done

echo "Restarting the agent while its durable queue contains unsent events"
kubectl --context "$CONTEXT" -n "$AGENT_NAMESPACE" rollout restart deployment/soak-agent-quellops-agent
kubectl --context "$CONTEXT" -n "$AGENT_NAMESPACE" rollout status deployment/soak-agent-quellops-agent --timeout=5m

for index in $(seq 13 24); do
  replicas=$((index % 3 + 1))
  kubectl --context "$CONTEXT" -n "$NAMESPACE" scale deployment/churn --replicas "$replicas" >/dev/null
  kubectl --context "$CONTEXT" -n "$NAMESPACE" annotate deployment/churn "soak.quellops.io/revision=${index}" --overwrite >/dev/null
done

start_receiver
minimum=$((baseline + 10))
wait_for_value accepted "$minimum" 180

echo "Running recovered churn for ${DURATION}s"
deadline=$((SECONDS + DURATION))
iteration=25
while ((SECONDS < deadline)); do
  replicas=$((iteration % 3 + 1))
  kubectl --context "$CONTEXT" -n "$NAMESPACE" scale deployment/churn --replicas "$replicas" >/dev/null
  kubectl --context "$CONTEXT" -n "$NAMESPACE" annotate deployment/churn "soak.quellops.io/revision=${iteration}" --overwrite >/dev/null
  iteration=$((iteration + 1))
  sleep 2
done

sleep 8
final_stats="$(curl -fsS "http://127.0.0.1:${PORT}/stats")"
printf '%s\n' "$final_stats" | python3 -m json.tool
printf '%s\n' "$final_stats" | python3 -c '
import json, sys
s = json.load(sys.stdin)
assert s["accepted"] >= 20, s
assert s["registrations"] == 1, s
assert s["heartbeats"] >= 1, s
assert s["last_dropped_events"] == 0, s
assert len(s["event_types"]) >= 3, s
print("Kubernetes soak assertions passed")
'

kubectl --context "$CONTEXT" get nodes
echo "Kubernetes multi-node load, outage, queue persistence, and restart simulation passed"
