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


def _cipher(master_key: str) -> AESGCM:
    try:
        key = base64.b64decode(master_key, validate=True)
    except (ValueError, binascii.Error) as exc:
        raise ValueError("AIDBG_MASTER_KEY must be base64 encoded") from exc
    if len(key) != 32:
        raise ValueError("AIDBG_MASTER_KEY must decode to exactly 32 bytes")
    return AESGCM(key)


class DatabaseStore:
    def __init__(self, path: str, master_key: str):
        self.path = Path(path)
        self._cipher = _cipher(master_key)

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
                CREATE TABLE IF NOT EXISTS database_sources (
                    name TEXT PRIMARY KEY,
                    provider TEXT NOT NULL,
                    engine TEXT NOT NULL,
                    access_mode TEXT NOT NULL,
                    service TEXT NOT NULL DEFAULT '',
                    config_json TEXT NOT NULL,
                    secret_nonce BLOB NOT NULL,
                    secret_ciphertext BLOB NOT NULL,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL
                );
                CREATE TABLE IF NOT EXISTS database_watchers (
                    source_name TEXT PRIMARY KEY,
                    enabled INTEGER NOT NULL DEFAULT 0,
                    interval_s INTEGER NOT NULL DEFAULT 60,
                    next_run_unix_s REAL,
                    last_started_unix_s REAL,
                    last_completed_unix_s REAL,
                    last_error TEXT,
                    consecutive_failures INTEGER NOT NULL DEFAULT 0,
                    total_runs INTEGER NOT NULL DEFAULT 0,
                    total_failures INTEGER NOT NULL DEFAULT 0,
                    total_requests INTEGER NOT NULL DEFAULT 0,
                    total_rows INTEGER NOT NULL DEFAULT 0,
                    total_bytes INTEGER NOT NULL DEFAULT 0,
                    estimated_cost_usd REAL NOT NULL DEFAULT 0,
                    lease_owner TEXT,
                    lease_until_unix_s REAL,
                    FOREIGN KEY(source_name) REFERENCES database_sources(name) ON DELETE CASCADE
                );
                CREATE TABLE IF NOT EXISTS database_signals (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    source_name TEXT NOT NULL,
                    observed_at_unix_ms INTEGER NOT NULL,
                    signal_json TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_database_signals_source_time
                    ON database_signals(source_name, observed_at_unix_ms DESC);
                """
            )
        os.chmod(self.path, 0o600)

    def _encrypt(self, name: str, credentials: dict[str, str]) -> tuple[bytes, bytes]:
        nonce = os.urandom(12)
        raw = json.dumps(credentials, sort_keys=True, separators=(",", ":")).encode()
        return nonce, self._cipher.encrypt(nonce, raw, ("database:" + name).encode())

    def _decrypt(self, row: sqlite3.Row) -> dict[str, str]:
        raw = self._cipher.decrypt(
            row["secret_nonce"],
            row["secret_ciphertext"],
            ("database:" + row["name"]).encode(),
        )
        credentials = json.loads(raw)
        if not isinstance(credentials, dict) or not all(
            isinstance(key, str) and isinstance(value, str)
            for key, value in credentials.items()
        ):
            raise ValueError("stored database credentials are invalid")
        return credentials

    def _public(self, row: sqlite3.Row, credentials: dict[str, str]) -> dict:
        return {
            "name": row["name"],
            "provider": row["provider"],
            "engine": row["engine"],
            "access_mode": row["access_mode"],
            "service": row["service"],
            "config": json.loads(row["config_json"]),
            "credential_keys": sorted(key for key, value in credentials.items() if value),
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }

    def list_sources(self) -> list[dict]:
        with self._connect() as connection:
            rows = connection.execute("SELECT * FROM database_sources ORDER BY name").fetchall()
        return [self._public(row, self._decrypt(row)) for row in rows]

    def get_source(self, name: str, *, include_credentials: bool = True) -> dict | None:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM database_sources WHERE name = ?", (name,)
            ).fetchone()
        if row is None:
            return None
        credentials = self._decrypt(row)
        result = self._public(row, credentials)
        if include_credentials:
            result["credentials"] = credentials
        return result

    def upsert_source(
        self,
        name: str,
        provider: str,
        engine: str,
        access_mode: str,
        service: str,
        config: dict,
        credentials: dict[str, str],
    ) -> dict:
        now = time.time()
        nonce, ciphertext = self._encrypt(name, credentials)
        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO database_sources (
                    name, provider, engine, access_mode, service, config_json,
                    secret_nonce, secret_ciphertext, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(name) DO UPDATE SET
                    provider=excluded.provider,
                    engine=excluded.engine,
                    access_mode=excluded.access_mode,
                    service=excluded.service,
                    config_json=excluded.config_json,
                    secret_nonce=excluded.secret_nonce,
                    secret_ciphertext=excluded.secret_ciphertext,
                    updated_at=excluded.updated_at
                """,
                (
                    name,
                    provider,
                    engine,
                    access_mode,
                    service,
                    json.dumps(config, sort_keys=True, separators=(",", ":")),
                    nonce,
                    ciphertext,
                    now,
                    now,
                ),
            )
        result = self.get_source(name, include_credentials=False)
        if result is None:
            raise RuntimeError("database source could not be saved")
        return result

    def delete_source(self, name: str) -> bool:
        with self._connect() as connection:
            connection.execute("DELETE FROM database_watchers WHERE source_name = ?", (name,))
            connection.execute("DELETE FROM database_signals WHERE source_name = ?", (name,))
            cursor = connection.execute("DELETE FROM database_sources WHERE name = ?", (name,))
        return cursor.rowcount > 0

    @staticmethod
    def _watcher(row: sqlite3.Row) -> dict:
        return {
            "source_name": row["source_name"],
            "enabled": bool(row["enabled"]),
            "interval_s": row["interval_s"],
            "next_run_unix_s": row["next_run_unix_s"],
            "last_started_unix_s": row["last_started_unix_s"],
            "last_completed_unix_s": row["last_completed_unix_s"],
            "last_error": row["last_error"],
            "consecutive_failures": row["consecutive_failures"],
            "total_runs": row["total_runs"],
            "total_failures": row["total_failures"],
            "total_requests": row["total_requests"],
            "total_rows": row["total_rows"],
            "total_bytes": row["total_bytes"],
            "estimated_cost_usd": row["estimated_cost_usd"],
        }

    def upsert_watcher(self, name: str, enabled: bool, interval_s: int) -> dict:
        now = time.time()
        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO database_watchers (source_name, enabled, interval_s, next_run_unix_s)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(source_name) DO UPDATE SET
                    enabled=excluded.enabled,
                    interval_s=excluded.interval_s,
                    next_run_unix_s=CASE
                        WHEN excluded.enabled = 1 THEN min(coalesce(database_watchers.next_run_unix_s, excluded.next_run_unix_s), excluded.next_run_unix_s)
                        ELSE NULL
                    END
                """,
                (name, int(enabled), interval_s, now if enabled else None),
            )
        result = self.get_watcher(name)
        if result is None:
            raise RuntimeError("database watcher could not be saved")
        return result

    def get_watcher(self, name: str) -> dict | None:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM database_watchers WHERE source_name = ?", (name,)
            ).fetchone()
        return self._watcher(row) if row is not None else None

    def list_watchers(self) -> list[dict]:
        with self._connect() as connection:
            rows = connection.execute("SELECT * FROM database_watchers ORDER BY source_name").fetchall()
        return [self._watcher(row) for row in rows]

    def claim_due(self, owner: str, now: float, lease_s: int = 90) -> list[str]:
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT source_name FROM database_watchers
                WHERE enabled = 1
                  AND coalesce(next_run_unix_s, 0) <= ?
                  AND (lease_until_unix_s IS NULL OR lease_until_unix_s < ?)
                ORDER BY coalesce(next_run_unix_s, 0)
                LIMIT 20
                """,
                (now, now),
            ).fetchall()
            claimed = []
            for row in rows:
                cursor = connection.execute(
                    """
                    UPDATE database_watchers SET lease_owner=?, lease_until_unix_s=?
                    WHERE source_name=? AND (lease_until_unix_s IS NULL OR lease_until_unix_s < ?)
                    """,
                    (owner, now + lease_s, row["source_name"], now),
                )
                if cursor.rowcount:
                    claimed.append(row["source_name"])
        return claimed

    def claim_one(self, name: str, owner: str, now: float, lease_s: int = 90) -> bool:
        with self._connect() as connection:
            cursor = connection.execute(
                """
                UPDATE database_watchers SET lease_owner=?, lease_until_unix_s=?
                WHERE source_name=? AND (lease_until_unix_s IS NULL OR lease_until_unix_s < ?)
                """,
                (owner, now + lease_s, name, now),
            )
        return cursor.rowcount > 0

    def started(self, name: str, owner: str, now: float) -> None:
        with self._connect() as connection:
            connection.execute(
                "UPDATE database_watchers SET last_started_unix_s=? WHERE source_name=? AND lease_owner=?",
                (now, name, owner),
            )

    def succeeded(
        self,
        name: str,
        owner: str,
        completed: float,
        requests: int,
        rows: int,
        bytes_received: int,
        cost: float,
    ) -> None:
        watcher = self.get_watcher(name)
        interval = int((watcher or {}).get("interval_s") or 60)
        with self._connect() as connection:
            connection.execute(
                """
                UPDATE database_watchers SET
                    next_run_unix_s=?, last_completed_unix_s=?, last_error=NULL,
                    consecutive_failures=0, total_runs=total_runs+1,
                    total_requests=total_requests+?, total_rows=total_rows+?,
                    total_bytes=total_bytes+?, estimated_cost_usd=estimated_cost_usd+?,
                    lease_owner=NULL, lease_until_unix_s=NULL
                WHERE source_name=? AND lease_owner=?
                """,
                (
                    completed + interval,
                    completed,
                    requests,
                    rows,
                    bytes_received,
                    cost,
                    name,
                    owner,
                ),
            )

    def failed(self, name: str, owner: str, error: str, now: float) -> None:
        watcher = self.get_watcher(name)
        failures = int((watcher or {}).get("consecutive_failures") or 0) + 1
        interval = int((watcher or {}).get("interval_s") or 60)
        backoff = min(interval * (2 ** (failures - 1)), 3600)
        with self._connect() as connection:
            connection.execute(
                """
                UPDATE database_watchers SET
                    next_run_unix_s=?, last_completed_unix_s=?, last_error=?,
                    consecutive_failures=?, total_failures=total_failures+1,
                    lease_owner=NULL, lease_until_unix_s=NULL
                WHERE source_name=? AND lease_owner=?
                """,
                (now + backoff, now, error[:1000], failures, name, owner),
            )

    def add_signals(self, name: str, signals: list[dict], retention: int = 5000) -> None:
        with self._connect() as connection:
            for signal in signals:
                connection.execute(
                    "INSERT INTO database_signals (source_name, observed_at_unix_ms, signal_json) VALUES (?, ?, ?)",
                    (
                        name,
                        int(signal["observed_at_unix_ms"]),
                        json.dumps(signal, sort_keys=True, separators=(",", ":")),
                    ),
                )
            connection.execute(
                """
                DELETE FROM database_signals WHERE id IN (
                    SELECT id FROM database_signals WHERE source_name=?
                    ORDER BY observed_at_unix_ms DESC LIMIT -1 OFFSET ?
                )
                """,
                (name, retention),
            )

    def list_signals(self, source: str | None = None, limit: int = 100) -> list[dict]:
        with self._connect() as connection:
            if source:
                rows = connection.execute(
                    "SELECT signal_json FROM database_signals WHERE source_name=? ORDER BY observed_at_unix_ms DESC LIMIT ?",
                    (source, limit),
                ).fetchall()
            else:
                rows = connection.execute(
                    "SELECT signal_json FROM database_signals ORDER BY observed_at_unix_ms DESC LIMIT ?",
                    (limit,),
                ).fetchall()
        return [json.loads(row["signal_json"]) for row in rows]


class _PostgresCompat:
    def __init__(self, connection):
        self.connection = connection

    @staticmethod
    def _sql(statement: str) -> str:
        statement = statement.replace("?", "%s")
        statement = statement.replace(
            "min(coalesce(database_watchers.next_run_unix_s, excluded.next_run_unix_s), excluded.next_run_unix_s)",
            "LEAST(coalesce(database_watchers.next_run_unix_s, excluded.next_run_unix_s), excluded.next_run_unix_s)",
        )
        return statement.replace("LIMIT -1 OFFSET %s", "OFFSET %s")

    def execute(self, statement: str, parameters=()):
        return self.connection.execute(self._sql(statement), parameters)


class PostgresDatabaseStore(DatabaseStore):
    """Shared control storage with the same behavior as the SQLite store."""

    def __init__(self, database_url: str, master_key: str):
        self.database_url = database_url
        self._cipher = _cipher(master_key)

    @contextmanager
    def _connect(self):
        import psycopg
        from psycopg.rows import dict_row

        connection = psycopg.connect(self.database_url, row_factory=dict_row)
        try:
            yield _PostgresCompat(connection)
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def initialize(self) -> None:
        statements = [
            """CREATE TABLE IF NOT EXISTS database_sources (
                name TEXT PRIMARY KEY, provider TEXT NOT NULL, engine TEXT NOT NULL,
                access_mode TEXT NOT NULL, service TEXT NOT NULL DEFAULT '', config_json TEXT NOT NULL,
                secret_nonce BYTEA NOT NULL, secret_ciphertext BYTEA NOT NULL,
                created_at DOUBLE PRECISION NOT NULL, updated_at DOUBLE PRECISION NOT NULL)""",
            """CREATE TABLE IF NOT EXISTS database_watchers (
                source_name TEXT PRIMARY KEY REFERENCES database_sources(name) ON DELETE CASCADE,
                enabled INTEGER NOT NULL DEFAULT 0, interval_s INTEGER NOT NULL DEFAULT 60,
                next_run_unix_s DOUBLE PRECISION, last_started_unix_s DOUBLE PRECISION,
                last_completed_unix_s DOUBLE PRECISION, last_error TEXT,
                consecutive_failures INTEGER NOT NULL DEFAULT 0, total_runs BIGINT NOT NULL DEFAULT 0,
                total_failures BIGINT NOT NULL DEFAULT 0, total_requests BIGINT NOT NULL DEFAULT 0,
                total_rows BIGINT NOT NULL DEFAULT 0, total_bytes BIGINT NOT NULL DEFAULT 0,
                estimated_cost_usd DOUBLE PRECISION NOT NULL DEFAULT 0,
                lease_owner TEXT, lease_until_unix_s DOUBLE PRECISION)""",
            """CREATE TABLE IF NOT EXISTS database_signals (
                id BIGSERIAL PRIMARY KEY, source_name TEXT NOT NULL,
                observed_at_unix_ms BIGINT NOT NULL, signal_json TEXT NOT NULL)""",
            "CREATE INDEX IF NOT EXISTS idx_database_signals_source_time ON database_signals(source_name, observed_at_unix_ms DESC)",
        ]
        with self._connect() as connection:
            for statement in statements:
                connection.execute(statement)


def create_database_store(path: str, master_key: str, database_url: str = "") -> DatabaseStore:
    if database_url:
        return PostgresDatabaseStore(database_url, master_key)
    return DatabaseStore(path, master_key)
