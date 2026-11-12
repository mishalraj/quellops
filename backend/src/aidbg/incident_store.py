from __future__ import annotations

import json
import sqlite3
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any


def _encoded(value: dict[str, Any]) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def is_meaningful_incident(incident: dict[str, Any]) -> bool:
    """Return true when a snapshot contains an actionable finding."""
    return bool(
        incident.get("anomalies")
        or incident.get("k8s_evidence")
        or incident.get("log_evidence")
        or any(
            cause.get("type") != "insufficient_telemetry"
            for cause in incident.get("top_causes", [])
        )
    )


class IncidentStore:
    """Durable incident snapshots shared by API and event workers."""

    def __init__(self, path: str, retention: int = 500):
        self.path = Path(path)
        self.retention = max(10, min(int(retention), 100_000))

    @contextmanager
    def _connect(self):
        connection = sqlite3.connect(self.path, timeout=10)
        connection.row_factory = sqlite3.Row
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
                CREATE TABLE IF NOT EXISTS incident_results (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    incident_key TEXT NOT NULL UNIQUE,
                    trigger_type TEXT NOT NULL,
                    trigger_id TEXT NOT NULL,
                    generated_at_unix_ms INTEGER NOT NULL,
                    incident_json TEXT NOT NULL,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_incident_results_generated
                    ON incident_results(generated_at_unix_ms DESC, id DESC);
                CREATE TABLE IF NOT EXISTS consumer_worker_status (
                    group_id TEXT PRIMARY KEY,
                    status_json TEXT NOT NULL,
                    updated_at REAL NOT NULL
                );
                """
            )

    def save(
        self,
        incident_key: str,
        incident: dict[str, Any],
        *,
        trigger_type: str,
        trigger_id: str,
    ) -> dict[str, Any]:
        now = time.time()
        generated = int(incident.get("generated_at_unix_ms") or now * 1000)
        with self._connect() as connection:
            connection.execute(
                """INSERT INTO incident_results
                   (incident_key,trigger_type,trigger_id,generated_at_unix_ms,
                    incident_json,created_at,updated_at)
                   VALUES (?,?,?,?,?,?,?)
                   ON CONFLICT(incident_key) DO UPDATE SET
                    trigger_type=excluded.trigger_type,
                    trigger_id=excluded.trigger_id,
                    generated_at_unix_ms=excluded.generated_at_unix_ms,
                    incident_json=excluded.incident_json,
                    updated_at=excluded.updated_at""",
                (
                    incident_key,
                    trigger_type,
                    trigger_id,
                    generated,
                    _encoded(incident),
                    now,
                    now,
                ),
            )
            connection.execute(
                """DELETE FROM incident_results WHERE id NOT IN (
                   SELECT id FROM incident_results
                   ORDER BY generated_at_unix_ms DESC,id DESC LIMIT ?)""",
                (self.retention,),
            )
        return incident

    @staticmethod
    def _result(row) -> dict[str, Any]:
        incident = json.loads(row["incident_json"])
        incident["persistence"] = {
            "incident_key": row["incident_key"],
            "trigger_type": row["trigger_type"],
            "trigger_id": row["trigger_id"],
            "stored_at_unix_s": row["updated_at"],
        }
        return incident

    def latest(self) -> dict[str, Any] | None:
        with self._connect() as connection:
            row = connection.execute(
                """SELECT * FROM incident_results
                   ORDER BY generated_at_unix_ms DESC,id DESC LIMIT 1"""
            ).fetchone()
        return self._result(row) if row else None

    def list(self, limit: int = 100) -> list[dict[str, Any]]:
        limit = max(1, min(int(limit), 1000))
        with self._connect() as connection:
            rows = connection.execute(
                """SELECT * FROM incident_results
                   ORDER BY generated_at_unix_ms DESC,id DESC LIMIT ?""",
                (limit,),
            ).fetchall()
        return [self._result(row) for row in rows]

    def save_worker_status(self, group_id: str, status: dict[str, Any]) -> None:
        now = time.time()
        with self._connect() as connection:
            connection.execute(
                """INSERT INTO consumer_worker_status (group_id,status_json,updated_at)
                   VALUES (?,?,?)
                   ON CONFLICT(group_id) DO UPDATE SET
                    status_json=excluded.status_json,
                    updated_at=excluded.updated_at""",
                (group_id, _encoded(status), now),
            )

    def worker_status(self, group_id: str, stale_after_s: float = 15) -> dict[str, Any] | None:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM consumer_worker_status WHERE group_id=?", (group_id,)
            ).fetchone()
        if row is None:
            return None
        result = json.loads(row["status_json"])
        result["reported_at"] = row["updated_at"]
        if time.time() - float(row["updated_at"]) > stale_after_s:
            result["healthy"] = False
            result["last_error"] = "consumer worker heartbeat is stale"
        return result


class PostgresIncidentStore(IncidentStore):
    def __init__(self, database_url: str, retention: int = 500):
        try:
            import psycopg
            from psycopg.rows import dict_row
        except ImportError as exc:
            raise RuntimeError("PostgreSQL incident storage requires psycopg") from exc
        self.database_url = database_url
        self.retention = max(10, min(int(retention), 100_000))
        self._psycopg = psycopg
        self._dict_row = dict_row

    @contextmanager
    def _connect(self):
        with self._psycopg.connect(self.database_url, row_factory=self._dict_row) as connection:
            with connection.transaction():
                yield connection

    def initialize(self) -> None:
        with self._connect() as connection:
            connection.execute("SELECT pg_advisory_xact_lock(684324674)")
            connection.execute(
                """CREATE TABLE IF NOT EXISTS incident_results (
                    id BIGSERIAL PRIMARY KEY,
                    incident_key TEXT NOT NULL UNIQUE,
                    trigger_type TEXT NOT NULL,
                    trigger_id TEXT NOT NULL,
                    generated_at_unix_ms BIGINT NOT NULL,
                    incident_json TEXT NOT NULL,
                    created_at DOUBLE PRECISION NOT NULL,
                    updated_at DOUBLE PRECISION NOT NULL
                )"""
            )
            connection.execute(
                """CREATE INDEX IF NOT EXISTS idx_incident_results_generated
                   ON incident_results(generated_at_unix_ms DESC,id DESC)"""
            )
            connection.execute(
                """CREATE TABLE IF NOT EXISTS consumer_worker_status (
                    group_id TEXT PRIMARY KEY,
                    status_json TEXT NOT NULL,
                    updated_at DOUBLE PRECISION NOT NULL
                )"""
            )

    def save(
        self,
        incident_key: str,
        incident: dict[str, Any],
        *,
        trigger_type: str,
        trigger_id: str,
    ) -> dict[str, Any]:
        now = time.time()
        generated = int(incident.get("generated_at_unix_ms") or now * 1000)
        with self._connect() as connection:
            connection.execute(
                """INSERT INTO incident_results
                   (incident_key,trigger_type,trigger_id,generated_at_unix_ms,
                    incident_json,created_at,updated_at)
                   VALUES (%s,%s,%s,%s,%s,%s,%s)
                   ON CONFLICT(incident_key) DO UPDATE SET
                    trigger_type=excluded.trigger_type,
                    trigger_id=excluded.trigger_id,
                    generated_at_unix_ms=excluded.generated_at_unix_ms,
                    incident_json=excluded.incident_json,
                    updated_at=excluded.updated_at""",
                (
                    incident_key,
                    trigger_type,
                    trigger_id,
                    generated,
                    _encoded(incident),
                    now,
                    now,
                ),
            )
            connection.execute(
                """DELETE FROM incident_results WHERE id NOT IN (
                   SELECT id FROM incident_results
                   ORDER BY generated_at_unix_ms DESC,id DESC LIMIT %s)""",
                (self.retention,),
            )
        return incident

    def list(self, limit: int = 100) -> list[dict[str, Any]]:
        limit = max(1, min(int(limit), 1000))
        with self._connect() as connection:
            rows = connection.execute(
                """SELECT * FROM incident_results
                   ORDER BY generated_at_unix_ms DESC,id DESC LIMIT %s""",
                (limit,),
            ).fetchall()
        return [self._result(row) for row in rows]

    def save_worker_status(self, group_id: str, status: dict[str, Any]) -> None:
        now = time.time()
        with self._connect() as connection:
            connection.execute(
                """INSERT INTO consumer_worker_status (group_id,status_json,updated_at)
                   VALUES (%s,%s,%s)
                   ON CONFLICT(group_id) DO UPDATE SET
                    status_json=excluded.status_json,
                    updated_at=excluded.updated_at""",
                (group_id, _encoded(status), now),
            )

    def worker_status(self, group_id: str, stale_after_s: float = 15) -> dict[str, Any] | None:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM consumer_worker_status WHERE group_id=%s", (group_id,)
            ).fetchone()
        if row is None:
            return None
        result = json.loads(row["status_json"])
        result["reported_at"] = row["updated_at"]
        if time.time() - float(row["updated_at"]) > stale_after_s:
            result["healthy"] = False
            result["last_error"] = "consumer worker heartbeat is stale"
        return result


def create_incident_store(path: str, database_url: str = "", retention: int = 500) -> IncidentStore:
    return (
        PostgresIncidentStore(database_url, retention)
        if database_url
        else IncidentStore(path, retention)
    )
