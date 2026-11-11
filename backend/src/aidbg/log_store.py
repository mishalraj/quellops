from __future__ import annotations

import base64
import binascii
import json
import os
import sqlite3
import time
from contextlib import contextmanager
from pathlib import Path

from cryptography.hazmat.primitives.ciphers.aead import AESGCM


class LogSourceStore:
    """SQLite source store with authenticated encryption for credentials."""

    def __init__(self, path: str, master_key: str):
        self.path = Path(path)
        try:
            key = base64.b64decode(master_key, validate=True)
        except (ValueError, binascii.Error) as exc:
            raise ValueError("AIDBG_MASTER_KEY must be base64 encoded") from exc
        if len(key) != 32:
            raise ValueError("AIDBG_MASTER_KEY must decode to exactly 32 bytes")
        self._cipher = AESGCM(key)

    def initialize(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as connection:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS log_sources (
                    name TEXT PRIMARY KEY,
                    provider TEXT NOT NULL,
                    service TEXT NOT NULL DEFAULT '',
                    config_json TEXT NOT NULL,
                    secret_nonce BLOB NOT NULL,
                    secret_ciphertext BLOB NOT NULL,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL
                )
                """
            )
            columns = {
                row["name"]
                for row in connection.execute("PRAGMA table_info(log_sources)").fetchall()
            }
            if "service" not in columns:
                connection.execute(
                    "ALTER TABLE log_sources ADD COLUMN service TEXT NOT NULL DEFAULT ''"
                )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS log_watchers (
                    source_name TEXT PRIMARY KEY,
                    enabled INTEGER NOT NULL,
                    interval_s INTEGER NOT NULL,
                    lag_s INTEGER NOT NULL,
                    initial_lookback_s INTEGER NOT NULL,
                    checkpoint_unix_s REAL,
                    next_run_unix_s REAL,
                    last_started_unix_s REAL,
                    last_completed_unix_s REAL,
                    last_error TEXT,
                    last_records INTEGER NOT NULL DEFAULT 0,
                    last_requests INTEGER NOT NULL DEFAULT 0,
                    last_bytes INTEGER NOT NULL DEFAULT 0,
                    lease_owner TEXT,
                    lease_until_unix_s REAL,
                    consecutive_failures INTEGER NOT NULL DEFAULT 0,
                    total_runs INTEGER NOT NULL DEFAULT 0,
                    total_failures INTEGER NOT NULL DEFAULT 0,
                    total_requests INTEGER NOT NULL DEFAULT 0,
                    total_bytes INTEGER NOT NULL DEFAULT 0,
                    total_records INTEGER NOT NULL DEFAULT 0,
                    total_throttles INTEGER NOT NULL DEFAULT 0,
                    last_cost_usd REAL NOT NULL DEFAULT 0,
                    estimated_cost_usd REAL NOT NULL DEFAULT 0
                )
                """
            )
            watcher_columns = {
                row["name"]
                for row in connection.execute("PRAGMA table_info(log_watchers)").fetchall()
            }
            for column, definition in (
                ("lease_owner", "TEXT"),
                ("lease_until_unix_s", "REAL"),
                ("consecutive_failures", "INTEGER NOT NULL DEFAULT 0"),
                ("total_runs", "INTEGER NOT NULL DEFAULT 0"),
                ("total_failures", "INTEGER NOT NULL DEFAULT 0"),
                ("total_requests", "INTEGER NOT NULL DEFAULT 0"),
                ("total_bytes", "INTEGER NOT NULL DEFAULT 0"),
                ("total_records", "INTEGER NOT NULL DEFAULT 0"),
                ("total_throttles", "INTEGER NOT NULL DEFAULT 0"),
                ("last_cost_usd", "REAL NOT NULL DEFAULT 0"),
                ("estimated_cost_usd", "REAL NOT NULL DEFAULT 0"),
            ):
                if column not in watcher_columns:
                    connection.execute(
                        f"ALTER TABLE log_watchers ADD COLUMN {column} {definition}"
                    )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS log_detections (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    source_name TEXT NOT NULL,
                    at_unix_ms INTEGER NOT NULL,
                    signal_json TEXT NOT NULL
                )
                """
            )
            connection.execute(
                "CREATE INDEX IF NOT EXISTS idx_log_detections_source_time "
                "ON log_detections(source_name, at_unix_ms DESC)"
            )
        os.chmod(self.path, 0o600)

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

    def _encrypt(self, name: str, credentials: dict[str, str]) -> tuple[bytes, bytes]:
        nonce = os.urandom(12)
        cleartext = json.dumps(
            credentials, sort_keys=True, separators=(",", ":")
        ).encode("utf-8")
        return nonce, self._cipher.encrypt(nonce, cleartext, name.encode("utf-8"))

    def _decrypt(self, row: sqlite3.Row) -> dict[str, str]:
        cleartext = self._cipher.decrypt(
            row["secret_nonce"],
            row["secret_ciphertext"],
            row["name"].encode("utf-8"),
        )
        value = json.loads(cleartext)
        if not isinstance(value, dict) or not all(
            isinstance(key, str) and isinstance(item, str)
            for key, item in value.items()
        ):
            raise ValueError("stored credentials are invalid")
        return value

    @staticmethod
    def _public(row: sqlite3.Row, credentials: dict[str, str]) -> dict:
        return {
            "name": row["name"],
            "provider": row["provider"],
            "service": row["service"],
            "config": json.loads(row["config_json"]),
            "credential_keys": sorted(key for key, value in credentials.items() if value),
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }

    def list(self) -> list[dict]:
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT * FROM log_sources ORDER BY name"
            ).fetchall()
        return [self._public(row, self._decrypt(row)) for row in rows]

    def get(self, name: str) -> dict | None:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM log_sources WHERE name = ?", (name,)
            ).fetchone()
        if row is None:
            return None
        credentials = self._decrypt(row)
        result = self._public(row, credentials)
        result["credentials"] = credentials
        return result

    def upsert(
        self,
        name: str,
        provider: str,
        config: dict,
        credentials: dict[str, str],
        service: str = "",
    ) -> dict:
        now = time.time()
        nonce, encrypted = self._encrypt(name, credentials)
        encoded_config = json.dumps(config, sort_keys=True, separators=(",", ":"))
        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO log_sources (
                    name, provider, service, config_json, secret_nonce,
                    secret_ciphertext, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(name) DO UPDATE SET
                    provider = excluded.provider,
                    service = excluded.service,
                    config_json = excluded.config_json,
                    secret_nonce = excluded.secret_nonce,
                    secret_ciphertext = excluded.secret_ciphertext,
                    updated_at = excluded.updated_at
                """,
                (name, provider, service, encoded_config, nonce, encrypted, now, now),
            )
        saved = self.get(name)
        if saved is None:
            raise RuntimeError("source could not be saved")
        saved.pop("credentials", None)
        return saved

    def delete(self, name: str) -> bool:
        with self._connect() as connection:
            connection.execute(
                "DELETE FROM log_watchers WHERE source_name = ?", (name,)
            )
            connection.execute(
                "DELETE FROM log_detections WHERE source_name = ?", (name,)
            )
            cursor = connection.execute(
                "DELETE FROM log_sources WHERE name = ?", (name,)
            )
        return cursor.rowcount > 0

    @staticmethod
    def _watcher(row: sqlite3.Row) -> dict:
        return {
            "source_name": row["source_name"],
            "enabled": bool(row["enabled"]),
            "interval_s": row["interval_s"],
            "lag_s": row["lag_s"],
            "initial_lookback_s": row["initial_lookback_s"],
            "checkpoint_unix_s": row["checkpoint_unix_s"],
            "next_run_unix_s": row["next_run_unix_s"],
            "last_started_unix_s": row["last_started_unix_s"],
            "last_completed_unix_s": row["last_completed_unix_s"],
            "last_error": row["last_error"],
            "last_records": row["last_records"],
            "last_requests": row["last_requests"],
            "last_bytes": row["last_bytes"],
            "consecutive_failures": row["consecutive_failures"],
            "total_runs": row["total_runs"],
            "total_failures": row["total_failures"],
            "total_requests": row["total_requests"],
            "total_bytes": row["total_bytes"],
            "total_records": row["total_records"],
            "total_throttles": row["total_throttles"],
            "last_cost_usd": row["last_cost_usd"],
            "estimated_cost_usd": row["estimated_cost_usd"],
        }

    def list_watchers(self) -> list[dict]:
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT * FROM log_watchers ORDER BY source_name"
            ).fetchall()
        return [self._watcher(row) for row in rows]

    def get_watcher(self, name: str) -> dict | None:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM log_watchers WHERE source_name = ?", (name,)
            ).fetchone()
        return self._watcher(row) if row is not None else None

    def upsert_watcher(
        self,
        name: str,
        *,
        enabled: bool,
        interval_s: int,
        lag_s: int,
        initial_lookback_s: int,
    ) -> dict:
        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO log_watchers (
                    source_name, enabled, interval_s, lag_s,
                    initial_lookback_s, next_run_unix_s
                ) VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(source_name) DO UPDATE SET
                    enabled = excluded.enabled,
                    interval_s = excluded.interval_s,
                    lag_s = excluded.lag_s,
                    initial_lookback_s = excluded.initial_lookback_s,
                    next_run_unix_s = CASE
                        WHEN excluded.enabled = 1 THEN 0
                        ELSE log_watchers.next_run_unix_s
                    END
                """,
                (name, int(enabled), interval_s, lag_s, initial_lookback_s, 0),
            )
        watcher = self.get_watcher(name)
        if watcher is None:
            raise RuntimeError("watcher could not be saved")
        return watcher

    def due_watchers(self, now: float) -> list[dict]:
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT * FROM log_watchers
                WHERE enabled = 1
                  AND (next_run_unix_s IS NULL OR next_run_unix_s <= ?)
                ORDER BY source_name
                """,
                (now,),
            ).fetchall()
        return [self._watcher(row) for row in rows]

    def claim_due_watchers(
        self, now: float, owner: str, *, lease_s: float = 120, limit: int = 20
    ) -> list[dict]:
        """Atomically reserve due work so multiple backend replicas cannot duplicate it."""
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            rows = connection.execute(
                """
                SELECT * FROM log_watchers
                WHERE enabled = 1
                  AND (next_run_unix_s IS NULL OR next_run_unix_s <= ?)
                  AND (lease_until_unix_s IS NULL OR lease_until_unix_s <= ?)
                ORDER BY source_name
                LIMIT ?
                """,
                (now, now, limit),
            ).fetchall()
            names = [row["source_name"] for row in rows]
            for name in names:
                connection.execute(
                    """
                    UPDATE log_watchers
                    SET lease_owner = ?, lease_until_unix_s = ?
                    WHERE source_name = ?
                      AND (lease_until_unix_s IS NULL OR lease_until_unix_s <= ?)
                    """,
                    (owner, now + lease_s, name, now),
                )
            if not names:
                return []
            placeholders = ",".join("?" for _ in names)
            claimed = connection.execute(
                f"SELECT * FROM log_watchers WHERE source_name IN ({placeholders}) "
                "AND lease_owner = ? ORDER BY source_name",
                (*names, owner),
            ).fetchall()
        return [self._watcher(row) for row in claimed]

    def claim_watcher(
        self, name: str, now: float, owner: str, *, lease_s: float = 120
    ) -> bool:
        with self._connect() as connection:
            cursor = connection.execute(
                """
                UPDATE log_watchers
                SET lease_owner = ?, lease_until_unix_s = ?
                WHERE source_name = ?
                  AND (lease_until_unix_s IS NULL OR lease_until_unix_s <= ? OR lease_owner = ?)
                """,
                (owner, now + lease_s, name, now, owner),
            )
        return cursor.rowcount == 1

    def watcher_started(self, name: str, now: float, next_run: float) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                UPDATE log_watchers
                SET last_started_unix_s = ?, next_run_unix_s = ?, last_error = NULL
                WHERE source_name = ?
                """,
                (now, next_run, name),
            )

    def watcher_succeeded(
        self,
        name: str,
        *,
        checkpoint: float,
        completed: float,
        next_run: float,
        records: int,
        requests: int,
        bytes_received: int,
        cost_usd: float = 0,
        owner: str | None = None,
    ) -> None:
        owner_clause = " AND lease_owner = ?" if owner else ""
        with self._connect() as connection:
            connection.execute(
                """
                UPDATE log_watchers SET
                    checkpoint_unix_s = ?, last_completed_unix_s = ?,
                    next_run_unix_s = ?, last_error = NULL,
                    last_records = ?, last_requests = ?, last_bytes = ?,
                    last_cost_usd = ?, total_runs = total_runs + 1,
                    total_requests = total_requests + ?, total_bytes = total_bytes + ?,
                    total_records = total_records + ?,
                    estimated_cost_usd = estimated_cost_usd + ?,
                    consecutive_failures = 0, lease_owner = NULL,
                    lease_until_unix_s = NULL
                WHERE source_name = ?
                """ + owner_clause,
                (
                    checkpoint, completed, next_run, records, requests, bytes_received,
                    cost_usd, requests, bytes_received, records, cost_usd, name,
                    *((owner,) if owner else ()),
                ),
            )

    def watcher_failed(
        self, name: str, *, error: str, next_run: float, owner: str | None = None
    ) -> None:
        owner_clause = " AND lease_owner = ?" if owner else ""
        with self._connect() as connection:
            connection.execute(
                """
                UPDATE log_watchers
                SET last_error = ?, next_run_unix_s = ?,
                    consecutive_failures = consecutive_failures + 1,
                    total_runs = total_runs + 1,
                    total_failures = total_failures + 1,
                    total_throttles = total_throttles + ?,
                    lease_owner = NULL, lease_until_unix_s = NULL
                WHERE source_name = ?
                """ + owner_clause,
                (
                    error[:1000], next_run,
                    int("429" in error or "throttl" in error.lower()),
                    name, *((owner,) if owner else ()),
                ),
            )

    def reset_watcher_checkpoint(self, name: str) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                UPDATE log_watchers
                SET checkpoint_unix_s = NULL, next_run_unix_s = 0,
                    last_error = NULL
                WHERE source_name = ?
                """,
                (name,),
            )

    def add_detection(self, signal: dict) -> None:
        source = str(signal.get("source") or "")
        if not source:
            return
        at_unix_ms = int(signal.get("at_unix_ms") or time.time() * 1000)
        encoded = json.dumps(signal, sort_keys=True, separators=(",", ":"))
        with self._connect() as connection:
            connection.execute(
                "INSERT INTO log_detections(source_name, at_unix_ms, signal_json) "
                "VALUES (?, ?, ?)",
                (source, at_unix_ms, encoded),
            )
            connection.execute(
                """
                DELETE FROM log_detections
                WHERE source_name = ? AND id NOT IN (
                    SELECT id FROM log_detections
                    WHERE source_name = ? ORDER BY at_unix_ms DESC, id DESC LIMIT 1000
                )
                """,
                (source, source),
            )

    def list_detections(
        self, source_name: str | None = None, *, limit: int = 100
    ) -> list[dict]:
        limit = max(1, min(limit, 1000))
        with self._connect() as connection:
            if source_name:
                rows = connection.execute(
                    "SELECT signal_json FROM log_detections WHERE source_name = ? "
                    "ORDER BY at_unix_ms DESC, id DESC LIMIT ?",
                    (source_name, limit),
                ).fetchall()
            else:
                rows = connection.execute(
                    "SELECT signal_json FROM log_detections "
                    "ORDER BY at_unix_ms DESC, id DESC LIMIT ?",
                    (limit,),
                ).fetchall()
        return [json.loads(row["signal_json"]) for row in rows]


class PostgresLogSourceStore(LogSourceStore):
    """PostgreSQL store for shared state across multiple backend replicas."""

    def __init__(self, database_url: str, master_key: str):
        if not database_url.startswith(("postgresql://", "postgres://")):
            raise ValueError("AIDBG_CONTROL_DATABASE_URL must be a PostgreSQL URL")
        self.database_url = database_url
        try:
            key = base64.b64decode(master_key, validate=True)
        except (ValueError, binascii.Error) as exc:
            raise ValueError("AIDBG_MASTER_KEY must be base64 encoded") from exc
        if len(key) != 32:
            raise ValueError("AIDBG_MASTER_KEY must decode to exactly 32 bytes")
        self._cipher = AESGCM(key)
        try:
            import psycopg
            from psycopg.rows import dict_row
        except ImportError as exc:
            raise RuntimeError(
                "PostgreSQL storage requires the psycopg package"
            ) from exc
        self._psycopg = psycopg
        self._dict_row = dict_row

    @contextmanager
    def _connect(self):
        with self._psycopg.connect(
            self.database_url, row_factory=self._dict_row, connect_timeout=10
        ) as connection:
            yield connection

    def initialize(self) -> None:
        with self._connect() as connection:
            # Only one replica runs schema migration statements at a time.
            connection.execute("SELECT pg_advisory_xact_lock(684324671)")
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS log_sources (
                    name TEXT PRIMARY KEY,
                    provider TEXT NOT NULL,
                    service TEXT NOT NULL DEFAULT '',
                    config_json TEXT NOT NULL,
                    secret_nonce BYTEA NOT NULL,
                    secret_ciphertext BYTEA NOT NULL,
                    created_at DOUBLE PRECISION NOT NULL,
                    updated_at DOUBLE PRECISION NOT NULL
                )
                """
            )
            connection.execute(
                "ALTER TABLE log_sources ADD COLUMN IF NOT EXISTS "
                "service TEXT NOT NULL DEFAULT ''"
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS log_watchers (
                    source_name TEXT PRIMARY KEY REFERENCES log_sources(name) ON DELETE CASCADE,
                    enabled BOOLEAN NOT NULL,
                    interval_s INTEGER NOT NULL,
                    lag_s INTEGER NOT NULL,
                    initial_lookback_s INTEGER NOT NULL,
                    checkpoint_unix_s DOUBLE PRECISION,
                    next_run_unix_s DOUBLE PRECISION,
                    last_started_unix_s DOUBLE PRECISION,
                    last_completed_unix_s DOUBLE PRECISION,
                    last_error TEXT,
                    last_records INTEGER NOT NULL DEFAULT 0,
                    last_requests INTEGER NOT NULL DEFAULT 0,
                    last_bytes BIGINT NOT NULL DEFAULT 0,
                    lease_owner TEXT,
                    lease_until_unix_s DOUBLE PRECISION,
                    consecutive_failures INTEGER NOT NULL DEFAULT 0,
                    total_runs BIGINT NOT NULL DEFAULT 0,
                    total_failures BIGINT NOT NULL DEFAULT 0,
                    total_requests BIGINT NOT NULL DEFAULT 0,
                    total_bytes BIGINT NOT NULL DEFAULT 0,
                    total_records BIGINT NOT NULL DEFAULT 0,
                    total_throttles BIGINT NOT NULL DEFAULT 0,
                    last_cost_usd DOUBLE PRECISION NOT NULL DEFAULT 0,
                    estimated_cost_usd DOUBLE PRECISION NOT NULL DEFAULT 0
                )
                """
            )
            for column, definition in (
                ("lease_owner", "TEXT"),
                ("lease_until_unix_s", "DOUBLE PRECISION"),
                ("consecutive_failures", "INTEGER NOT NULL DEFAULT 0"),
                ("total_runs", "BIGINT NOT NULL DEFAULT 0"),
                ("total_failures", "BIGINT NOT NULL DEFAULT 0"),
                ("total_requests", "BIGINT NOT NULL DEFAULT 0"),
                ("total_bytes", "BIGINT NOT NULL DEFAULT 0"),
                ("total_records", "BIGINT NOT NULL DEFAULT 0"),
                ("total_throttles", "BIGINT NOT NULL DEFAULT 0"),
                ("last_cost_usd", "DOUBLE PRECISION NOT NULL DEFAULT 0"),
                ("estimated_cost_usd", "DOUBLE PRECISION NOT NULL DEFAULT 0"),
            ):
                connection.execute(
                    f"ALTER TABLE log_watchers ADD COLUMN IF NOT EXISTS {column} {definition}"
                )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS log_detections (
                    id BIGSERIAL PRIMARY KEY,
                    source_name TEXT NOT NULL REFERENCES log_sources(name) ON DELETE CASCADE,
                    at_unix_ms BIGINT NOT NULL,
                    signal_json TEXT NOT NULL
                )
                """
            )
            connection.execute(
                "CREATE INDEX IF NOT EXISTS idx_log_detections_source_time "
                "ON log_detections(source_name, at_unix_ms DESC)"
            )

    def list(self) -> list[dict]:
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT * FROM log_sources ORDER BY name"
            ).fetchall()
        return [self._public(row, self._decrypt(row)) for row in rows]

    def get(self, name: str) -> dict | None:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM log_sources WHERE name = %s", (name,)
            ).fetchone()
        if row is None:
            return None
        credentials = self._decrypt(row)
        result = self._public(row, credentials)
        result["credentials"] = credentials
        return result

    def upsert(
        self,
        name: str,
        provider: str,
        config: dict,
        credentials: dict[str, str],
        service: str = "",
    ) -> dict:
        now = time.time()
        nonce, encrypted = self._encrypt(name, credentials)
        encoded_config = json.dumps(config, sort_keys=True, separators=(",", ":"))
        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO log_sources (
                    name, provider, service, config_json, secret_nonce,
                    secret_ciphertext, created_at, updated_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT(name) DO UPDATE SET
                    provider = excluded.provider,
                    service = excluded.service,
                    config_json = excluded.config_json,
                    secret_nonce = excluded.secret_nonce,
                    secret_ciphertext = excluded.secret_ciphertext,
                    updated_at = excluded.updated_at
                """,
                (name, provider, service, encoded_config, nonce, encrypted, now, now),
            )
        saved = self.get(name)
        if saved is None:
            raise RuntimeError("source could not be saved")
        saved.pop("credentials", None)
        return saved

    def delete(self, name: str) -> bool:
        with self._connect() as connection:
            cursor = connection.execute(
                "DELETE FROM log_sources WHERE name = %s", (name,)
            )
        return cursor.rowcount > 0

    def list_watchers(self) -> list[dict]:
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT * FROM log_watchers ORDER BY source_name"
            ).fetchall()
        return [self._watcher(row) for row in rows]

    def get_watcher(self, name: str) -> dict | None:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM log_watchers WHERE source_name = %s", (name,)
            ).fetchone()
        return self._watcher(row) if row is not None else None

    def upsert_watcher(
        self,
        name: str,
        *,
        enabled: bool,
        interval_s: int,
        lag_s: int,
        initial_lookback_s: int,
    ) -> dict:
        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO log_watchers (
                    source_name, enabled, interval_s, lag_s,
                    initial_lookback_s, next_run_unix_s
                ) VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT(source_name) DO UPDATE SET
                    enabled = excluded.enabled,
                    interval_s = excluded.interval_s,
                    lag_s = excluded.lag_s,
                    initial_lookback_s = excluded.initial_lookback_s,
                    next_run_unix_s = CASE
                        WHEN excluded.enabled THEN 0
                        ELSE log_watchers.next_run_unix_s
                    END
                """,
                (name, enabled, interval_s, lag_s, initial_lookback_s, 0),
            )
        watcher = self.get_watcher(name)
        if watcher is None:
            raise RuntimeError("watcher could not be saved")
        return watcher

    def due_watchers(self, now: float) -> list[dict]:
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT * FROM log_watchers
                WHERE enabled = TRUE
                  AND (next_run_unix_s IS NULL OR next_run_unix_s <= %s)
                ORDER BY source_name
                """,
                (now,),
            ).fetchall()
        return [self._watcher(row) for row in rows]

    def claim_due_watchers(
        self, now: float, owner: str, *, lease_s: float = 120, limit: int = 20
    ) -> list[dict]:
        with self._connect() as connection:
            rows = connection.execute(
                """
                WITH due AS (
                    SELECT source_name FROM log_watchers
                    WHERE enabled = TRUE
                      AND (next_run_unix_s IS NULL OR next_run_unix_s <= %s)
                      AND (lease_until_unix_s IS NULL OR lease_until_unix_s <= %s)
                    ORDER BY source_name
                    FOR UPDATE SKIP LOCKED
                    LIMIT %s
                )
                UPDATE log_watchers AS watcher
                SET lease_owner = %s, lease_until_unix_s = %s
                FROM due
                WHERE watcher.source_name = due.source_name
                RETURNING watcher.*
                """,
                (now, now, limit, owner, now + lease_s),
            ).fetchall()
        rows.sort(key=lambda row: row["source_name"])
        return [self._watcher(row) for row in rows]

    def claim_watcher(
        self, name: str, now: float, owner: str, *, lease_s: float = 120
    ) -> bool:
        with self._connect() as connection:
            cursor = connection.execute(
                """
                UPDATE log_watchers
                SET lease_owner = %s, lease_until_unix_s = %s
                WHERE source_name = %s
                  AND (lease_until_unix_s IS NULL OR lease_until_unix_s <= %s OR lease_owner = %s)
                """,
                (owner, now + lease_s, name, now, owner),
            )
        return cursor.rowcount == 1

    def watcher_started(self, name: str, now: float, next_run: float) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                UPDATE log_watchers
                SET last_started_unix_s = %s, next_run_unix_s = %s, last_error = NULL
                WHERE source_name = %s
                """,
                (now, next_run, name),
            )

    def watcher_succeeded(
        self,
        name: str,
        *,
        checkpoint: float,
        completed: float,
        next_run: float,
        records: int,
        requests: int,
        bytes_received: int,
        cost_usd: float = 0,
        owner: str | None = None,
    ) -> None:
        owner_clause = " AND lease_owner = %s" if owner else ""
        with self._connect() as connection:
            connection.execute(
                """
                UPDATE log_watchers SET
                    checkpoint_unix_s = %s, last_completed_unix_s = %s,
                    next_run_unix_s = %s, last_error = NULL,
                    last_records = %s, last_requests = %s, last_bytes = %s,
                    last_cost_usd = %s, total_runs = total_runs + 1,
                    total_requests = total_requests + %s, total_bytes = total_bytes + %s,
                    total_records = total_records + %s,
                    estimated_cost_usd = estimated_cost_usd + %s,
                    consecutive_failures = 0, lease_owner = NULL,
                    lease_until_unix_s = NULL
                WHERE source_name = %s
                """ + owner_clause,
                (
                    checkpoint,
                    completed,
                    next_run,
                    records,
                    requests,
                    bytes_received,
                    cost_usd,
                    requests,
                    bytes_received,
                    records,
                    cost_usd,
                    name,
                    *((owner,) if owner else ()),
                ),
            )

    def watcher_failed(
        self, name: str, *, error: str, next_run: float, owner: str | None = None
    ) -> None:
        owner_clause = " AND lease_owner = %s" if owner else ""
        with self._connect() as connection:
            connection.execute(
                """
                UPDATE log_watchers
                SET last_error = %s, next_run_unix_s = %s,
                    consecutive_failures = consecutive_failures + 1,
                    total_runs = total_runs + 1,
                    total_failures = total_failures + 1,
                    total_throttles = total_throttles + %s,
                    lease_owner = NULL, lease_until_unix_s = NULL
                WHERE source_name = %s
                """ + owner_clause,
                (
                    error[:1000], next_run,
                    int("429" in error or "throttl" in error.lower()),
                    name, *((owner,) if owner else ()),
                ),
            )

    def reset_watcher_checkpoint(self, name: str) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                UPDATE log_watchers
                SET checkpoint_unix_s = NULL, next_run_unix_s = 0,
                    last_error = NULL
                WHERE source_name = %s
                """,
                (name,),
            )

    def add_detection(self, signal: dict) -> None:
        source = str(signal.get("source") or "")
        if not source:
            return
        at_unix_ms = int(signal.get("at_unix_ms") or time.time() * 1000)
        encoded = json.dumps(signal, sort_keys=True, separators=(",", ":"))
        with self._connect() as connection:
            connection.execute(
                "INSERT INTO log_detections(source_name, at_unix_ms, signal_json) "
                "VALUES (%s, %s, %s)",
                (source, at_unix_ms, encoded),
            )
            connection.execute(
                """
                DELETE FROM log_detections
                WHERE source_name = %s AND id NOT IN (
                    SELECT id FROM log_detections
                    WHERE source_name = %s ORDER BY at_unix_ms DESC, id DESC LIMIT 1000
                )
                """,
                (source, source),
            )

    def list_detections(
        self, source_name: str | None = None, *, limit: int = 100
    ) -> list[dict]:
        limit = max(1, min(limit, 1000))
        with self._connect() as connection:
            if source_name:
                rows = connection.execute(
                    "SELECT signal_json FROM log_detections WHERE source_name = %s "
                    "ORDER BY at_unix_ms DESC, id DESC LIMIT %s",
                    (source_name, limit),
                ).fetchall()
            else:
                rows = connection.execute(
                    "SELECT signal_json FROM log_detections "
                    "ORDER BY at_unix_ms DESC, id DESC LIMIT %s",
                    (limit,),
                ).fetchall()
        return [json.loads(row["signal_json"]) for row in rows]


def create_log_source_store(
    path: str, master_key: str, database_url: str = ""
) -> LogSourceStore:
    if database_url:
        return PostgresLogSourceStore(database_url, master_key)
    return LogSourceStore(path, master_key)
