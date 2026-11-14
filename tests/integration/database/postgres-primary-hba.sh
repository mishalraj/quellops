#!/usr/bin/env bash
set -euo pipefail

cat >>"$PGDATA/pg_hba.conf" <<'EOF'
host replication replicator 0.0.0.0/0 scram-sha-256
host replication replicator ::/0 scram-sha-256
EOF
