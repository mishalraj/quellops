from __future__ import annotations

import json
import sqlite3
import time
from contextlib import contextmanager
from pathlib import Path


def _json(value) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


class KubernetesStore:
    """SQLite control store for registered Kubernetes agents and normalized events."""

    def __init__(self, path: str, retention_events: int = 5000):
        self.path = Path(path)
        self.retention_events = max(100, min(int(retention_events), 1_000_000))

    @contextmanager
    def _connect(self):
        connection = sqlite3.connect(self.path, timeout=10)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        try:
            yield connection
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def initialize(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS k8s_clusters (
                    cluster_id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    agent_version TEXT NOT NULL DEFAULT '',
                    kubernetes_version TEXT NOT NULL DEFAULT '',
                    namespaces_json TEXT NOT NULL DEFAULT '[]',
                    workloads_json TEXT NOT NULL DEFAULT '[]',
                    selected_namespaces_json TEXT NOT NULL DEFAULT '[]',
                    config_revision INTEGER NOT NULL DEFAULT 1,
                    last_seen_unix_s REAL NOT NULL,
                    status TEXT NOT NULL DEFAULT 'online',
                    last_error TEXT,
                    queue_depth INTEGER NOT NULL DEFAULT 0,
                    dropped_events INTEGER NOT NULL DEFAULT 0,
                    agent_token_hash TEXT,
                    previous_agent_token_hash TEXT,
                    previous_agent_token_valid_until REAL,
                    credential_rotated_at REAL,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL
                );
                CREATE TABLE IF NOT EXISTS k8s_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    cluster_id TEXT NOT NULL,
                    event_id TEXT NOT NULL,
                    at_unix_ms INTEGER NOT NULL,
                    namespace TEXT NOT NULL DEFAULT '',
                    event_type TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    received_at REAL NOT NULL,
                    transported_at REAL,
                    transport_attempts INTEGER NOT NULL DEFAULT 0,
                    transport_claim_owner TEXT,
                    transport_claim_until REAL,
                    transport_last_error TEXT,
                    UNIQUE(cluster_id, event_id),
                    FOREIGN KEY(cluster_id) REFERENCES k8s_clusters(cluster_id) ON DELETE CASCADE
                );
                CREATE INDEX IF NOT EXISTS idx_k8s_events_cluster_time
                    ON k8s_events(cluster_id, at_unix_ms DESC);
                CREATE INDEX IF NOT EXISTS idx_k8s_events_id ON k8s_events(id);
                """
            )
            columns = {
                row["name"]
                for row in connection.execute("PRAGMA table_info(k8s_events)").fetchall()
            }
            cluster_columns = {
                row["name"]
                for row in connection.execute("PRAGMA table_info(k8s_clusters)").fetchall()
            }
            cluster_migrations = {
                "agent_token_hash": "ALTER TABLE k8s_clusters ADD COLUMN agent_token_hash TEXT",
                "previous_agent_token_hash": "ALTER TABLE k8s_clusters ADD COLUMN previous_agent_token_hash TEXT",
                "previous_agent_token_valid_until": "ALTER TABLE k8s_clusters ADD COLUMN previous_agent_token_valid_until REAL",
                "credential_rotated_at": "ALTER TABLE k8s_clusters ADD COLUMN credential_rotated_at REAL",
            }
            for column, statement in cluster_migrations.items():
                if column not in cluster_columns:
                    connection.execute(statement)
            migrations = {
                "transported_at": "ALTER TABLE k8s_events ADD COLUMN transported_at REAL",
                "transport_attempts": "ALTER TABLE k8s_events ADD COLUMN transport_attempts INTEGER NOT NULL DEFAULT 0",
                "transport_claim_owner": "ALTER TABLE k8s_events ADD COLUMN transport_claim_owner TEXT",
                "transport_claim_until": "ALTER TABLE k8s_events ADD COLUMN transport_claim_until REAL",
                "transport_last_error": "ALTER TABLE k8s_events ADD COLUMN transport_last_error TEXT",
            }
            for column, statement in migrations.items():
                if column not in columns:
                    connection.execute(statement)
            connection.execute(
                "CREATE INDEX IF NOT EXISTS idx_k8s_events_transport "
                "ON k8s_events(transported_at, transport_claim_until, id)"
            )

    @staticmethod
    def _cluster(row) -> dict:
        result = dict(row)
        has_credential = bool(result.pop("agent_token_hash", None))
        result.pop("previous_agent_token_hash", None)
        result.pop("previous_agent_token_valid_until", None)
        result["credential_mode"] = "individual" if has_credential else "bootstrap"
        for key in ("namespaces", "workloads", "selected_namespaces"):
            result[key] = json.loads(result.pop(key + "_json") or "[]")
        result["online"] = time.time() - float(result["last_seen_unix_s"]) <= 90
        return result

    def agent_credentials(self, cluster_id: str) -> dict | None:
        with self._connect() as connection:
            row = connection.execute(
                """SELECT agent_token_hash,previous_agent_token_hash,
                   previous_agent_token_valid_until,credential_rotated_at
                   FROM k8s_clusters WHERE cluster_id=?""",
                (cluster_id,),
            ).fetchone()
        return dict(row) if row else None

    def rotate_agent_token(
        self, cluster_id: str, token_hash: str, *, grace_s: int = 600
    ) -> bool:
        now = time.time()
        with self._connect() as connection:
            cursor = connection.execute(
                """UPDATE k8s_clusters SET
                   previous_agent_token_hash=agent_token_hash,
                   previous_agent_token_valid_until=CASE WHEN agent_token_hash IS NULL THEN NULL ELSE ? END,
                   agent_token_hash=?,credential_rotated_at=?,updated_at=?
                   WHERE cluster_id=?""",
                (now + max(0, grace_s), token_hash, now, now, cluster_id),
            )
        return cursor.rowcount == 1

    def register(self, cluster: dict) -> dict:
        now = time.time()
        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO k8s_clusters (
                    cluster_id, name, agent_version, kubernetes_version,
                    namespaces_json, workloads_json, last_seen_unix_s, status,
                    last_error, queue_depth, dropped_events, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, 'online', ?, ?, ?, ?, ?)
                ON CONFLICT(cluster_id) DO UPDATE SET
                    name = excluded.name,
                    agent_version = excluded.agent_version,
                    kubernetes_version = excluded.kubernetes_version,
                    namespaces_json = excluded.namespaces_json,
                    workloads_json = excluded.workloads_json,
                    last_seen_unix_s = excluded.last_seen_unix_s,
                    status = 'online',
                    last_error = excluded.last_error,
                    queue_depth = excluded.queue_depth,
                    dropped_events = excluded.dropped_events,
                    updated_at = excluded.updated_at
                """,
                (
                    cluster["cluster_id"],
                    cluster["name"],
                    cluster["agent_version"],
                    cluster["kubernetes_version"],
                    _json(cluster["namespaces"]),
                    _json(cluster["workloads"]),
                    now,
                    cluster.get("last_error"),
                    cluster.get("queue_depth", 0),
                    cluster.get("dropped_events", 0),
                    now,
                    now,
                ),
            )
        return self.get(cluster["cluster_id"])

    def heartbeat(
        self,
        cluster_id: str,
        *,
        queue_depth: int,
        dropped_events: int,
        last_error: str | None,
    ) -> dict | None:
        now = time.time()
        with self._connect() as connection:
            connection.execute(
                """UPDATE k8s_clusters SET last_seen_unix_s = ?, status = 'online',
                   queue_depth = ?, dropped_events = ?, last_error = ?, updated_at = ?
                   WHERE cluster_id = ?""",
                (now, queue_depth, dropped_events, last_error, now, cluster_id),
            )
        return self.get(cluster_id)

    def get(self, cluster_id: str) -> dict | None:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM k8s_clusters WHERE cluster_id = ?", (cluster_id,)
            ).fetchone()
        return self._cluster(row) if row else None

    def list(self) -> list[dict]:
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT * FROM k8s_clusters ORDER BY name, cluster_id"
            ).fetchall()
        return [self._cluster(row) for row in rows]

    def update_scope(self, cluster_id: str, namespaces: list[str]) -> dict | None:
        now = time.time()
        with self._connect() as connection:
            connection.execute(
                """UPDATE k8s_clusters SET selected_namespaces_json = ?,
                   config_revision = config_revision + 1, updated_at = ?
                   WHERE cluster_id = ?""",
                (_json(namespaces), now, cluster_id),
            )
        return self.get(cluster_id)

    def delete(self, cluster_id: str) -> bool:
        with self._connect() as connection:
            cursor = connection.execute(
                "DELETE FROM k8s_clusters WHERE cluster_id = ?", (cluster_id,)
            )
        return cursor.rowcount == 1

    def add_events(self, cluster_id: str, events: list[dict]) -> tuple[int, int]:
        accepted = 0
        duplicates = 0
        now = time.time()
        with self._connect() as connection:
            for event in events:
                cursor = connection.execute(
                    """INSERT OR IGNORE INTO k8s_events
                       (cluster_id, event_id, at_unix_ms, namespace, event_type, payload_json, received_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?)""",
                    (
                        cluster_id,
                        event["event_id"],
                        event["at_unix_ms"],
                        event.get("namespace") or "",
                        event["type"],
                        _json(event),
                        now,
                    ),
                )
                if cursor.rowcount:
                    accepted += 1
                else:
                    duplicates += 1
            connection.execute(
                """DELETE FROM k8s_events WHERE cluster_id = ?
                   AND transported_at IS NOT NULL AND id NOT IN (
                       SELECT id FROM k8s_events WHERE cluster_id = ?
                       ORDER BY id DESC LIMIT ?
                   )""",
                (cluster_id, cluster_id, self.retention_events),
            )
        return accepted, duplicates

    def list_events(
        self,
        cluster_id: str | None = None,
        *,
        limit: int = 100,
        after_id: int = 0,
        ascending: bool = False,
    ) -> list[dict]:
        limit = max(1, min(limit, 1000))
        clauses = ["id > ?"]
        args: list = [after_id]
        if cluster_id:
            clauses.append("cluster_id = ?")
            args.append(cluster_id)
        order = "ASC" if ascending else "DESC"
        args.append(limit)
        with self._connect() as connection:
            rows = connection.execute(
                f"SELECT id, cluster_id, payload_json, received_at FROM k8s_events "
                f"WHERE {' AND '.join(clauses)} ORDER BY id {order} LIMIT ?",
                args,
            ).fetchall()
        result = []
        for row in rows:
            payload = json.loads(row["payload_json"])
            payload["id"] = row["id"]
            payload["cluster_id"] = row["cluster_id"]
            payload["received_at"] = row["received_at"]
            result.append(payload)
        return result

    def claim_transport_batch(
        self, owner: str, *, limit: int = 100, lease_s: int = 30
    ) -> list[dict]:
        """Lease unpublished outbox rows to one backend replica."""
        now = time.time()
        limit = max(1, min(limit, 1000))
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            rows = connection.execute(
                """SELECT id FROM k8s_events
                   WHERE transported_at IS NULL
                     AND (transport_claim_until IS NULL OR transport_claim_until < ?)
                   ORDER BY id ASC LIMIT ?""",
                (now, limit),
            ).fetchall()
            ids = [int(row["id"]) for row in rows]
            if not ids:
                return []
            placeholders = ",".join("?" for _ in ids)
            connection.execute(
                f"""UPDATE k8s_events SET transport_claim_owner=?, transport_claim_until=?,
                    transport_attempts=transport_attempts+1, transport_last_error=NULL
                    WHERE id IN ({placeholders})""",
                [owner, now + lease_s, *ids],
            )
            claimed = connection.execute(
                f"""SELECT id,cluster_id,event_id,payload_json,transport_attempts
                    FROM k8s_events WHERE id IN ({placeholders}) ORDER BY id""",
                ids,
            ).fetchall()
        return [
            {
                "id": int(row["id"]),
                "cluster_id": row["cluster_id"],
                "event_id": row["event_id"],
                "event": json.loads(row["payload_json"]),
                "attempts": int(row["transport_attempts"]),
            }
            for row in claimed
        ]

    def mark_transport_complete(self, owner: str, ids: list[int]) -> None:
        if not ids:
            return
        placeholders = ",".join("?" for _ in ids)
        with self._connect() as connection:
            connection.execute(
                f"""UPDATE k8s_events SET transported_at=?, transport_claim_owner=NULL,
                    transport_claim_until=NULL, transport_last_error=NULL
                    WHERE transport_claim_owner=? AND id IN ({placeholders})""",
                [time.time(), owner, *ids],
            )

    def mark_events_local(self, cluster_id: str, event_ids: list[str]) -> None:
        """Mark rows complete when the database bridge is the selected transport."""
        if not event_ids:
            return
        placeholders = ",".join("?" for _ in event_ids)
        with self._connect() as connection:
            connection.execute(
                f"""UPDATE k8s_events SET transported_at=?,transport_last_error=NULL
                    WHERE cluster_id=? AND event_id IN ({placeholders})""",
                [time.time(), cluster_id, *event_ids],
            )

    def release_transport(self, owner: str, ids: list[int], error: str) -> None:
        if not ids:
            return
        placeholders = ",".join("?" for _ in ids)
        with self._connect() as connection:
            connection.execute(
                f"""UPDATE k8s_events SET transport_claim_owner=NULL,
                    transport_claim_until=NULL, transport_last_error=?
                    WHERE transport_claim_owner=? AND id IN ({placeholders})""",
                [error[:1000], owner, *ids],
            )

    def count_transport_backlog(self) -> int:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT COUNT(*) AS count FROM k8s_events WHERE transported_at IS NULL"
            ).fetchone()
        return int(row["count"])

    def count_events(self, cluster_id: str | None = None) -> int:
        with self._connect() as connection:
            if cluster_id:
                row = connection.execute(
                    "SELECT COUNT(*) AS count FROM k8s_events WHERE cluster_id = ?",
                    (cluster_id,),
                ).fetchone()
            else:
                row = connection.execute(
                    "SELECT COUNT(*) AS count FROM k8s_events"
                ).fetchone()
        return int(row["count"])


class PostgresKubernetesStore(KubernetesStore):
    def __init__(self, database_url: str, retention_events: int = 5000):
        if not database_url.startswith(("postgresql://", "postgres://")):
            raise ValueError("AIDBG_CONTROL_DATABASE_URL must be a PostgreSQL URL")
        try:
            import psycopg
            from psycopg.rows import dict_row
        except ImportError as exc:
            raise RuntimeError("PostgreSQL storage requires psycopg") from exc
        self.database_url = database_url
        self.retention_events = max(100, min(int(retention_events), 1_000_000))
        self._psycopg = psycopg
        self._dict_row = dict_row

    @contextmanager
    def _connect(self):
        with self._psycopg.connect(
            self.database_url, row_factory=self._dict_row
        ) as connection:
            with connection.transaction():
                yield connection

    def initialize(self) -> None:
        with self._connect() as connection:
            connection.execute("SELECT pg_advisory_xact_lock(684324672)")
            connection.execute(
                """CREATE TABLE IF NOT EXISTS k8s_clusters (
                    cluster_id TEXT PRIMARY KEY, name TEXT NOT NULL,
                    agent_version TEXT NOT NULL DEFAULT '', kubernetes_version TEXT NOT NULL DEFAULT '',
                    namespaces_json TEXT NOT NULL DEFAULT '[]', workloads_json TEXT NOT NULL DEFAULT '[]',
                    selected_namespaces_json TEXT NOT NULL DEFAULT '[]', config_revision BIGINT NOT NULL DEFAULT 1,
                    last_seen_unix_s DOUBLE PRECISION NOT NULL, status TEXT NOT NULL DEFAULT 'online',
                    last_error TEXT, queue_depth BIGINT NOT NULL DEFAULT 0,
                    dropped_events BIGINT NOT NULL DEFAULT 0, created_at DOUBLE PRECISION NOT NULL,
                    agent_token_hash TEXT, previous_agent_token_hash TEXT,
                    previous_agent_token_valid_until DOUBLE PRECISION,
                    credential_rotated_at DOUBLE PRECISION,
                    updated_at DOUBLE PRECISION NOT NULL
                )"""
            )
            connection.execute(
                """CREATE TABLE IF NOT EXISTS k8s_events (
                    id BIGSERIAL PRIMARY KEY, cluster_id TEXT NOT NULL REFERENCES k8s_clusters(cluster_id) ON DELETE CASCADE,
                    event_id TEXT NOT NULL, at_unix_ms BIGINT NOT NULL, namespace TEXT NOT NULL DEFAULT '',
                    event_type TEXT NOT NULL, payload_json TEXT NOT NULL, received_at DOUBLE PRECISION NOT NULL,
                    transported_at DOUBLE PRECISION, transport_attempts BIGINT NOT NULL DEFAULT 0,
                    transport_claim_owner TEXT, transport_claim_until DOUBLE PRECISION,
                    transport_last_error TEXT,
                    UNIQUE(cluster_id, event_id)
                )"""
            )
            connection.execute(
                "CREATE INDEX IF NOT EXISTS idx_k8s_events_cluster_time "
                "ON k8s_events(cluster_id, at_unix_ms DESC)"
            )
            connection.execute(
                "CREATE INDEX IF NOT EXISTS idx_k8s_events_id ON k8s_events(id)"
            )
            for column, definition in (
                ("agent_token_hash", "TEXT"),
                ("previous_agent_token_hash", "TEXT"),
                ("previous_agent_token_valid_until", "DOUBLE PRECISION"),
                ("credential_rotated_at", "DOUBLE PRECISION"),
            ):
                connection.execute(
                    f"ALTER TABLE k8s_clusters ADD COLUMN IF NOT EXISTS {column} {definition}"
                )
            connection.execute(
                "ALTER TABLE k8s_events ADD COLUMN IF NOT EXISTS transported_at DOUBLE PRECISION"
            )
            connection.execute(
                "ALTER TABLE k8s_events ADD COLUMN IF NOT EXISTS transport_attempts BIGINT NOT NULL DEFAULT 0"
            )
            connection.execute(
                "ALTER TABLE k8s_events ADD COLUMN IF NOT EXISTS transport_claim_owner TEXT"
            )
            connection.execute(
                "ALTER TABLE k8s_events ADD COLUMN IF NOT EXISTS transport_claim_until DOUBLE PRECISION"
            )
            connection.execute(
                "ALTER TABLE k8s_events ADD COLUMN IF NOT EXISTS transport_last_error TEXT"
            )
            connection.execute(
                "CREATE INDEX IF NOT EXISTS idx_k8s_events_transport "
                "ON k8s_events(transported_at,transport_claim_until,id)"
            )

    def register(self, cluster: dict) -> dict:
        now = time.time()
        with self._connect() as connection:
            connection.execute(
                """INSERT INTO k8s_clusters (
                    cluster_id, name, agent_version, kubernetes_version, namespaces_json,
                    workloads_json, last_seen_unix_s, status, last_error, queue_depth,
                    dropped_events, created_at, updated_at
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,'online',%s,%s,%s,%s,%s)
                ON CONFLICT(cluster_id) DO UPDATE SET name=excluded.name,
                    agent_version=excluded.agent_version, kubernetes_version=excluded.kubernetes_version,
                    namespaces_json=excluded.namespaces_json, workloads_json=excluded.workloads_json,
                    last_seen_unix_s=excluded.last_seen_unix_s, status='online',
                    last_error=excluded.last_error, queue_depth=excluded.queue_depth,
                    dropped_events=excluded.dropped_events, updated_at=excluded.updated_at""",
                (
                    cluster["cluster_id"],
                    cluster["name"],
                    cluster["agent_version"],
                    cluster["kubernetes_version"],
                    _json(cluster["namespaces"]),
                    _json(cluster["workloads"]),
                    now,
                    cluster.get("last_error"),
                    cluster.get("queue_depth", 0),
                    cluster.get("dropped_events", 0),
                    now,
                    now,
                ),
            )
        return self.get(cluster["cluster_id"])

    def agent_credentials(self, cluster_id: str) -> dict | None:
        with self._connect() as connection:
            row = connection.execute(
                """SELECT agent_token_hash,previous_agent_token_hash,
                   previous_agent_token_valid_until,credential_rotated_at
                   FROM k8s_clusters WHERE cluster_id=%s""",
                (cluster_id,),
            ).fetchone()
        return dict(row) if row else None

    def rotate_agent_token(
        self, cluster_id: str, token_hash: str, *, grace_s: int = 600
    ) -> bool:
        now = time.time()
        with self._connect() as connection:
            cursor = connection.execute(
                """UPDATE k8s_clusters SET
                   previous_agent_token_hash=agent_token_hash,
                   previous_agent_token_valid_until=CASE WHEN agent_token_hash IS NULL THEN NULL ELSE %s END,
                   agent_token_hash=%s,credential_rotated_at=%s,updated_at=%s
                   WHERE cluster_id=%s""",
                (now + max(0, grace_s), token_hash, now, now, cluster_id),
            )
        return cursor.rowcount == 1

    def heartbeat(
        self,
        cluster_id: str,
        *,
        queue_depth: int,
        dropped_events: int,
        last_error: str | None,
    ) -> dict | None:
        now = time.time()
        with self._connect() as connection:
            connection.execute(
                """UPDATE k8s_clusters SET last_seen_unix_s=%s,status='online',queue_depth=%s,
                   dropped_events=%s,last_error=%s,updated_at=%s WHERE cluster_id=%s""",
                (now, queue_depth, dropped_events, last_error, now, cluster_id),
            )
        return self.get(cluster_id)

    def get(self, cluster_id: str) -> dict | None:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM k8s_clusters WHERE cluster_id=%s", (cluster_id,)
            ).fetchone()
        return self._cluster(row) if row else None

    def list(self) -> list[dict]:
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT * FROM k8s_clusters ORDER BY name,cluster_id"
            ).fetchall()
        return [self._cluster(row) for row in rows]

    def update_scope(self, cluster_id: str, namespaces: list[str]) -> dict | None:
        with self._connect() as connection:
            connection.execute(
                """UPDATE k8s_clusters SET selected_namespaces_json=%s,
                   config_revision=config_revision+1,updated_at=%s WHERE cluster_id=%s""",
                (_json(namespaces), time.time(), cluster_id),
            )
        return self.get(cluster_id)

    def delete(self, cluster_id: str) -> bool:
        with self._connect() as connection:
            cursor = connection.execute(
                "DELETE FROM k8s_clusters WHERE cluster_id=%s", (cluster_id,)
            )
        return cursor.rowcount == 1

    def add_events(self, cluster_id: str, events: list[dict]) -> tuple[int, int]:
        accepted = duplicates = 0
        now = time.time()
        with self._connect() as connection:
            for event in events:
                cursor = connection.execute(
                    """INSERT INTO k8s_events
                       (cluster_id,event_id,at_unix_ms,namespace,event_type,payload_json,received_at)
                       VALUES (%s,%s,%s,%s,%s,%s,%s) ON CONFLICT(cluster_id,event_id) DO NOTHING""",
                    (
                        cluster_id,
                        event["event_id"],
                        event["at_unix_ms"],
                        event.get("namespace") or "",
                        event["type"],
                        _json(event),
                        now,
                    ),
                )
                if cursor.rowcount:
                    accepted += 1
                else:
                    duplicates += 1
            connection.execute(
                """DELETE FROM k8s_events WHERE cluster_id=%s
                   AND transported_at IS NOT NULL AND id NOT IN (
                   SELECT id FROM k8s_events WHERE cluster_id=%s ORDER BY id DESC LIMIT %s)""",
                (cluster_id, cluster_id, self.retention_events),
            )
        return accepted, duplicates

    def list_events(
        self,
        cluster_id: str | None = None,
        *,
        limit: int = 100,
        after_id: int = 0,
        ascending: bool = False,
    ) -> list[dict]:
        limit = max(1, min(limit, 1000))
        clauses = ["id > %s"]
        args: list = [after_id]
        if cluster_id:
            clauses.append("cluster_id = %s")
            args.append(cluster_id)
        order = "ASC" if ascending else "DESC"
        args.append(limit)
        with self._connect() as connection:
            rows = connection.execute(
                f"SELECT id,cluster_id,payload_json,received_at FROM k8s_events WHERE {' AND '.join(clauses)} ORDER BY id {order} LIMIT %s",
                args,
            ).fetchall()
        result = []
        for row in rows:
            payload = json.loads(row["payload_json"])
            payload["id"] = row["id"]
            payload["cluster_id"] = row["cluster_id"]
            payload["received_at"] = row["received_at"]
            result.append(payload)
        return result

    def count_events(self, cluster_id: str | None = None) -> int:
        with self._connect() as connection:
            if cluster_id:
                row = connection.execute(
                    "SELECT COUNT(*) AS count FROM k8s_events WHERE cluster_id = %s",
                    (cluster_id,),
                ).fetchone()
            else:
                row = connection.execute(
                    "SELECT COUNT(*) AS count FROM k8s_events"
                ).fetchone()
        return int(row["count"])

    def claim_transport_batch(
        self, owner: str, *, limit: int = 100, lease_s: int = 30
    ) -> list[dict]:
        now = time.time()
        limit = max(1, min(limit, 1000))
        with self._connect() as connection:
            rows = connection.execute(
                """WITH candidates AS (
                       SELECT id FROM k8s_events
                       WHERE transported_at IS NULL
                         AND (transport_claim_until IS NULL OR transport_claim_until < %s)
                       ORDER BY id FOR UPDATE SKIP LOCKED LIMIT %s
                   )
                   UPDATE k8s_events AS event SET transport_claim_owner=%s,
                       transport_claim_until=%s, transport_attempts=event.transport_attempts+1,
                       transport_last_error=NULL
                   FROM candidates WHERE event.id=candidates.id
                   RETURNING event.id,event.cluster_id,event.event_id,event.payload_json,event.transport_attempts""",
                (now, limit, owner, now + lease_s),
            ).fetchall()
        return [
            {
                "id": int(row["id"]),
                "cluster_id": row["cluster_id"],
                "event_id": row["event_id"],
                "event": json.loads(row["payload_json"]),
                "attempts": int(row["transport_attempts"]),
            }
            for row in rows
        ]

    def mark_transport_complete(self, owner: str, ids: list[int]) -> None:
        if not ids:
            return
        with self._connect() as connection:
            connection.execute(
                """UPDATE k8s_events SET transported_at=%s,transport_claim_owner=NULL,
                   transport_claim_until=NULL,transport_last_error=NULL
                   WHERE transport_claim_owner=%s AND id=ANY(%s)""",
                (time.time(), owner, ids),
            )

    def mark_events_local(self, cluster_id: str, event_ids: list[str]) -> None:
        if not event_ids:
            return
        with self._connect() as connection:
            connection.execute(
                """UPDATE k8s_events SET transported_at=%s,transport_last_error=NULL
                   WHERE cluster_id=%s AND event_id=ANY(%s)""",
                (time.time(), cluster_id, event_ids),
            )

    def release_transport(self, owner: str, ids: list[int], error: str) -> None:
        if not ids:
            return
        with self._connect() as connection:
            connection.execute(
                """UPDATE k8s_events SET transport_claim_owner=NULL,transport_claim_until=NULL,
                   transport_last_error=%s WHERE transport_claim_owner=%s AND id=ANY(%s)""",
                (error[:1000], owner, ids),
            )

    def count_transport_backlog(self) -> int:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT COUNT(*) AS count FROM k8s_events WHERE transported_at IS NULL"
            ).fetchone()
        return int(row["count"])


def create_kubernetes_store(
    path: str, database_url: str = "", retention_events: int = 5000
) -> KubernetesStore:
    return (
        PostgresKubernetesStore(database_url, retention_events)
        if database_url
        else KubernetesStore(path, retention_events)
    )
