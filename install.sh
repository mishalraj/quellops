#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT_DIR"
DOCKER=(docker)
STORAGE="${AIDBG_STORAGE:-sqlite}"
EVENT_TRANSPORT="${AIDBG_EVENT_TRANSPORT:-database}"
for argument in "$@"; do
  case "$argument" in
    --postgres) STORAGE="postgres" ;;
    --sqlite) STORAGE="sqlite" ;;
    --kafka) EVENT_TRANSPORT="kafka" ;;
    --database-events) EVENT_TRANSPORT="database" ;;
    *)
      echo "Usage: ./install.sh [--sqlite|--postgres] [--kafka|--database-events]"
      exit 1
      ;;
  esac
done
if [[ "$STORAGE" != "sqlite" && "$STORAGE" != "postgres" ]]; then
  echo "AIDBG_STORAGE must be sqlite or postgres"
  exit 1
fi
if [[ "$EVENT_TRANSPORT" != "database" && "$EVENT_TRANSPORT" != "kafka" ]]; then
  echo "AIDBG_EVENT_TRANSPORT must be database or kafka"
  exit 1
fi

install_docker() {
  case "$(uname -s)" in
    Darwin)
      command -v brew >/dev/null 2>&1 || {
        echo "Docker Desktop is required. Install Homebrew first: https://brew.sh"
        exit 1
      }
      brew install --cask docker
      open -a Docker
      ;;
    Linux)
      if command -v apt-get >/dev/null 2>&1; then
        sudo apt-get update
        sudo apt-get install -y docker.io docker-compose-plugin
        sudo systemctl enable --now docker
        sudo usermod -aG docker "$USER"
        DOCKER=(sudo docker)
      elif command -v dnf >/dev/null 2>&1; then
        sudo dnf install -y docker docker-compose-plugin
        sudo systemctl enable --now docker
        sudo usermod -aG docker "$USER"
        DOCKER=(sudo docker)
      else
        echo "Automatic Docker installation supports apt and dnf systems."
        exit 1
      fi
      ;;
    *)
      echo "Install Docker Desktop, then run this script again."
      exit 1
      ;;
  esac
}

if ! command -v docker >/dev/null 2>&1; then
  install_docker
fi

echo "Waiting for Docker..."
for _ in $(seq 1 60); do
  if "${DOCKER[@]}" info >/dev/null 2>&1; then
    break
  fi
  if [[ "$(uname -s)" == "Linux" ]] && sudo docker info >/dev/null 2>&1; then
    DOCKER=(sudo docker)
    break
  fi
  sleep 2
done
"${DOCKER[@]}" info >/dev/null 2>&1 || {
  echo "Docker is installed but not ready. Start Docker and run ./install.sh again."
  exit 1
}
"${DOCKER[@]}" compose version >/dev/null 2>&1 || {
  echo "Docker Compose v2 is required."
  exit 1
}
command -v openssl >/dev/null 2>&1 || {
  echo "OpenSSL is required to generate local secrets."
  exit 1
}

if [[ ! -f .env ]]; then
  cat > .env <<EOF
AIDBG_LOG_SERVICE_TOKEN=$(openssl rand -hex 32)
AIDBG_LOG_API_TOKEN=$(openssl rand -hex 32)
AIDBG_ADMIN_API_TOKEN=$(openssl rand -hex 24)
AIDBG_CHANGE_INGEST_TOKEN=$(openssl rand -hex 32)
AIDBG_GITHUB_WEBHOOK_SECRET=$(openssl rand -hex 32)
AIDBG_K8S_AGENT_TOKEN=$(openssl rand -hex 32)
AIDBG_MASTER_KEY=$(openssl rand -base64 32 | tr -d '\n')
AIDBG_POSTGRES_PASSWORD=$(openssl rand -hex 32)
EOF
  chmod 600 .env
fi

if ! grep -q '^AIDBG_CHANGE_INGEST_TOKEN=' .env; then
  printf '\nAIDBG_CHANGE_INGEST_TOKEN=%s\n' "$(openssl rand -hex 32)" >> .env
fi
if ! grep -q '^AIDBG_GITHUB_WEBHOOK_SECRET=' .env; then
  printf '\nAIDBG_GITHUB_WEBHOOK_SECRET=%s\n' "$(openssl rand -hex 32)" >> .env
fi

if ! grep -q '^AIDBG_K8S_AGENT_TOKEN=' .env; then
  printf '\nAIDBG_K8S_AGENT_TOKEN=%s\n' "$(openssl rand -hex 32)" >> .env
fi

if ! grep -q '^AIDBG_POSTGRES_PASSWORD=' .env; then
  printf '\nAIDBG_POSTGRES_PASSWORD=%s\n' "$(openssl rand -hex 32)" >> .env
fi
POSTGRES_PASSWORD="$(sed -n 's/^AIDBG_POSTGRES_PASSWORD=//p' .env | tail -n 1)"
grep -Ev '^(AIDBG_CONTROL_DATABASE_URL|AIDBG_EVENT_TRANSPORT|AIDBG_KAFKA_BOOTSTRAP_SERVERS|AIDBG_KAFKA_K8S_TOPIC|AIDBG_KAFKA_CONSUMER_GROUP|AIDBG_KAFKA_CONSUMER_EMBEDDED|AIDBG_INCIDENT_RETENTION)=' .env > .env.tmp || true
if [[ "$STORAGE" == "postgres" ]]; then
  printf 'AIDBG_CONTROL_DATABASE_URL=postgresql://aidbg:%s@aidbg-postgres:5432/aidbg\n' "$POSTGRES_PASSWORD" >> .env.tmp
else
  printf 'AIDBG_CONTROL_DATABASE_URL=\n' >> .env.tmp
fi
printf 'AIDBG_EVENT_TRANSPORT=%s\n' "$EVENT_TRANSPORT" >> .env.tmp
if [[ "$EVENT_TRANSPORT" == "kafka" ]]; then
  printf 'AIDBG_KAFKA_BOOTSTRAP_SERVERS=quellops-redpanda:9092\n' >> .env.tmp
else
  printf 'AIDBG_KAFKA_BOOTSTRAP_SERVERS=\n' >> .env.tmp
fi
printf 'AIDBG_KAFKA_K8S_TOPIC=quellops.kubernetes.events.v1\n' >> .env.tmp
printf 'AIDBG_KAFKA_CONSUMER_GROUP=quellops-central-v1\n' >> .env.tmp
printf 'AIDBG_KAFKA_CONSUMER_EMBEDDED=false\n' >> .env.tmp
printf 'AIDBG_INCIDENT_RETENTION=500\n' >> .env.tmp
mv .env.tmp .env
chmod 600 .env

COMPOSE_PROFILES=(--profile logs)
if [[ "$STORAGE" == "postgres" ]]; then
  COMPOSE_PROFILES+=(--profile postgres)
fi
if [[ "$EVENT_TRANSPORT" == "kafka" ]]; then
  COMPOSE_PROFILES+=(--profile kafka)
fi
"${DOCKER[@]}" compose "${COMPOSE_PROFILES[@]}" up -d --build

echo "Waiting for QuellOps..."
for _ in $(seq 1 60); do
  if curl --fail --silent http://127.0.0.1:8088/healthz >/dev/null; then
    break
  fi
  sleep 2
done
curl --fail --silent http://127.0.0.1:8088/healthz >/dev/null || {
  "${DOCKER[@]}" compose "${COMPOSE_PROFILES[@]}" logs --tail=100
  exit 1
}

ADMIN_TOKEN="$(sed -n 's/^AIDBG_ADMIN_API_TOKEN=//p' .env)"
echo "QuellOps is ready: http://127.0.0.1:8088/ui/"
echo "Logs setup: http://127.0.0.1:8088/ui/logs.html"
echo "Kubernetes setup: http://127.0.0.1:8088/ui/kubernetes.html"
echo "Administrator token: $ADMIN_TOKEN"
echo "Control storage: $STORAGE"
echo "Event transport: $EVENT_TRANSPORT"

case "$(uname -s)" in
  Darwin) open http://127.0.0.1:8088/ui/logs.html ;;
  Linux) command -v xdg-open >/dev/null 2>&1 && xdg-open http://127.0.0.1:8088/ui/logs.html >/dev/null 2>&1 || true ;;
esac
