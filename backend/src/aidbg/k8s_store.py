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

    def __init__(self, path: str):
        self.path = Path(path)

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
                    UNIQUE(cluster_id, event_id),
                    FOREIGN KEY(cluster_id) REFERENCES k8s_clusters(cluster_id) ON DELETE CASCADE
                );
                CREATE INDEX IF NOT EXISTS idx_k8s_events_cluster_time
                    ON k8s_events(cluster_id, at_unix_ms DESC);
                CREATE INDEX IF NOT EXISTS idx_k8s_events_id ON k8s_events(id);
                """
            )

    @staticmethod
    def _cluster(row) -> dict:
        result = dict(row)
        for key in ("namespaces", "workloads", "selected_namespaces"):
            result[key] = json.loads(result.pop(key + "_json") or "[]")
        result["online"] = time.time() - float(result["last_seen_unix_s"]) <= 90
        return result

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
                """DELETE FROM k8s_events WHERE cluster_id = ? AND id NOT IN (
                       SELECT id FROM k8s_events WHERE cluster_id = ?
                       ORDER BY id DESC LIMIT 5000
                   )""",
                (cluster_id, cluster_id),
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
    def __init__(self, database_url: str):
        if not database_url.startswith(("postgresql://", "postgres://")):
            raise ValueError("AIDBG_CONTROL_DATABASE_URL must be a PostgreSQL URL")
        try:
            import psycopg
            from psycopg.rows import dict_row
        except ImportError as exc:
            raise RuntimeError("PostgreSQL storage requires psycopg") from exc
        self.database_url = database_url
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
                    updated_at DOUBLE PRECISION NOT NULL
                )"""
            )
            connection.execute(
                """CREATE TABLE IF NOT EXISTS k8s_events (
                    id BIGSERIAL PRIMARY KEY, cluster_id TEXT NOT NULL REFERENCES k8s_clusters(cluster_id) ON DELETE CASCADE,
                    event_id TEXT NOT NULL, at_unix_ms BIGINT NOT NULL, namespace TEXT NOT NULL DEFAULT '',
                    event_type TEXT NOT NULL, payload_json TEXT NOT NULL, received_at DOUBLE PRECISION NOT NULL,
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
                """DELETE FROM k8s_events WHERE cluster_id=%s AND id NOT IN (
                   SELECT id FROM k8s_events WHERE cluster_id=%s ORDER BY id DESC LIMIT 5000)""",
                (cluster_id, cluster_id),
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


def create_kubernetes_store(path: str, database_url: str = "") -> KubernetesStore:
    return PostgresKubernetesStore(database_url) if database_url else KubernetesStore(path)
