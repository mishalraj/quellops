#!/usr/bin/env bash
set -euo pipefail

DATA_DIR="${PGDATA:-/var/lib/postgresql/data/pgdata}"
mkdir -p "$DATA_DIR"
chown -R postgres:postgres "$DATA_DIR"
chmod 700 "$DATA_DIR"

until gosu postgres pg_isready -h postgres-primary -U postgres -d postgres >/dev/null 2>&1; do
  sleep 1
done

if [[ ! -s "$DATA_DIR/PG_VERSION" ]]; then
  rm -rf "${DATA_DIR:?}"/*
  gosu postgres env PGPASSWORD=replica-test pg_basebackup \
    -h postgres-primary -U replicator -D "$DATA_DIR" -Fp -Xs -P -R
fi

exec gosu postgres postgres -D "$DATA_DIR" -c hot_standby=on
