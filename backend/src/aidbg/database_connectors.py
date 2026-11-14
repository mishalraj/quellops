from __future__ import annotations

import hashlib
import json
import re
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Protocol

ACCESS_MODES = {"cloud_metrics", "replica", "cdc"}

ENGINE_CATALOG: dict[str, dict[str, Any]] = {
    "postgresql": {"label": "PostgreSQL", "family": "sql", "replica": True},
    "mysql": {"label": "MySQL", "family": "sql", "replica": True},
    "mariadb": {"label": "MariaDB", "family": "sql", "replica": True},
    "sqlserver": {"label": "Microsoft SQL Server", "family": "sql", "replica": True},
    "oracle": {"label": "Oracle Database", "family": "sql", "replica": True},
    "mongodb": {"label": "MongoDB", "family": "nosql", "replica": True},
    "redis": {"label": "Redis", "family": "nosql", "replica": True},
    "cassandra": {"label": "Cassandra", "family": "nosql", "replica": False},
    "elasticsearch": {"label": "Elasticsearch", "family": "search", "replica": False},
    "aurora-postgresql": {"label": "Amazon Aurora PostgreSQL", "family": "sql"},
    "aurora-mysql": {"label": "Amazon Aurora MySQL", "family": "sql"},
    "rds-postgresql": {"label": "Amazon RDS PostgreSQL", "family": "sql"},
    "rds-mysql": {"label": "Amazon RDS MySQL", "family": "sql"},
    "rds-sqlserver": {"label": "Amazon RDS SQL Server", "family": "sql"},
    "rds-oracle": {"label": "Amazon RDS Oracle", "family": "sql"},
    "dynamodb": {"label": "Amazon DynamoDB", "family": "nosql"},
    "documentdb": {"label": "Amazon DocumentDB", "family": "nosql"},
    "elasticache-redis": {"label": "Amazon ElastiCache for Redis", "family": "nosql"},
    "redshift": {"label": "Amazon Redshift", "family": "warehouse"},
    "cloudsql-postgresql": {"label": "Google Cloud SQL PostgreSQL", "family": "sql"},
    "cloudsql-mysql": {"label": "Google Cloud SQL MySQL", "family": "sql"},
    "cloudsql-sqlserver": {"label": "Google Cloud SQL SQL Server", "family": "sql"},
    "alloydb": {"label": "Google AlloyDB", "family": "sql"},
    "spanner": {"label": "Google Cloud Spanner", "family": "sql"},
    "firestore": {"label": "Google Firestore", "family": "nosql"},
    "memorystore-redis": {"label": "Google Memorystore for Redis", "family": "nosql"},
    "azure-sql": {"label": "Azure SQL Database", "family": "sql"},
    "azure-postgresql": {"label": "Azure Database for PostgreSQL", "family": "sql"},
    "azure-mysql": {"label": "Azure Database for MySQL", "family": "sql"},
    "cosmosdb": {"label": "Azure Cosmos DB", "family": "nosql"},
    "azure-cache-redis": {"label": "Azure Cache for Redis", "family": "nosql"},
    "oci-autonomous": {"label": "Oracle Autonomous Database", "family": "sql"},
    "oci-database": {"label": "Oracle Cloud Database", "family": "sql"},
    "oci-nosql": {"label": "Oracle NoSQL Database", "family": "nosql"},
}

PROVIDER_CATALOG: dict[str, dict[str, Any]] = {
    "self_hosted": {
        "label": "Self-hosted or directly reachable replica",
        "modes": ["replica", "cdc"],
    },
    "aws": {"label": "Amazon Web Services", "modes": ["cloud_metrics", "replica", "cdc"]},
    "gcp": {"label": "Google Cloud", "modes": ["cloud_metrics", "replica", "cdc"]},
    "azure": {"label": "Microsoft Azure", "modes": ["cloud_metrics", "replica", "cdc"]},
    "oci": {"label": "Oracle Cloud Infrastructure", "modes": ["cloud_metrics", "replica", "cdc"]},
}

SAFE_NAME = re.compile(r"^[A-Za-z0-9_.-]{1,160}$")
WRITE_WORDS = re.compile(
    r"\b(insert|update|delete|merge|alter|create|drop|truncate|grant|revoke|execute|call|copy)\b",
    re.IGNORECASE,
)


class DatabaseSafetyError(ValueError):
    pass


class ConnectorUnavailable(RuntimeError):
    pass


@dataclass(frozen=True)
class DatabaseSignal:
    source: str
    provider: str
    engine: str
    resource: str
    database: str
    signal: str
    severity: str
    value: float | None
    unit: str
    observed_at_unix_ms: int
    evidence: dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return {
            "source": self.source,
            "provider": self.provider,
            "engine": self.engine,
            "resource": self.resource,
            "database": self.database,
            "signal": self.signal,
            "severity": self.severity,
            "value": self.value,
            "unit": self.unit,
            "observed_at_unix_ms": self.observed_at_unix_ms,
            "evidence": self.evidence,
        }


@dataclass
class CollectionResult:
    signals: list[DatabaseSignal]
    requests: int = 0
    rows: int = 0
    bytes_received: int = 0
    duration_ms: float = 0
    estimated_cost_usd: float = 0


class DatabaseConnector(Protocol):
    def verify(self) -> dict[str, Any]: ...

    def discover(self) -> list[dict[str, str]]: ...

    def collect(self) -> CollectionResult: ...


def validate_source(
    name: str,
    provider: str,
    engine: str,
    access_mode: str,
    config: dict[str, Any],
) -> dict[str, Any]:
    if not SAFE_NAME.fullmatch(name):
        raise DatabaseSafetyError(
            "source name may contain only letters, numbers, dots, dashes and underscores"
        )
    if provider not in PROVIDER_CATALOG:
        raise DatabaseSafetyError("unsupported database provider")
    if engine not in ENGINE_CATALOG:
        raise DatabaseSafetyError("unsupported database engine")
    if access_mode not in ACCESS_MODES:
        raise DatabaseSafetyError("access_mode must be cloud_metrics, replica, or cdc")
    if access_mode not in PROVIDER_CATALOG[provider]["modes"]:
        raise DatabaseSafetyError("provider does not support the selected access mode")
    normalized = dict(config)
    normalized["timeout_s"] = bounded_int(config.get("timeout_s", 5), 1, 30, "timeout_s")
    normalized["max_connections"] = bounded_int(
        config.get("max_connections", 2), 1, 5, "max_connections"
    )
    normalized["max_rows"] = bounded_int(config.get("max_rows", 100), 1, 1000, "max_rows")
    normalized["interval_s"] = bounded_int(config.get("interval_s", 60), 15, 86400, "interval_s")
    normalized["max_cycle_s"] = bounded_int(config.get("max_cycle_s", 15), 1, 60, "max_cycle_s")
    normalized["max_requests"] = bounded_int(config.get("max_requests", 10), 1, 50, "max_requests")
    normalized["enabled"] = bool(config.get("enabled", False))
    if access_mode == "replica":
        if not ENGINE_CATALOG[engine].get("replica", False):
            raise DatabaseSafetyError(
                "this engine cannot be monitored through the replica connector"
            )
        if not config.get("host"):
            raise DatabaseSafetyError("replica mode requires a host")
        if bool(config.get("allow_primary", False)):
            raise DatabaseSafetyError("primary database monitoring cannot be enabled")
        normalized["require_replica"] = True
        normalized["read_only"] = True
    elif access_mode == "cloud_metrics":
        if provider == "self_hosted":
            raise DatabaseSafetyError("cloud metrics require AWS, GCP, Azure, or OCI")
        if not config.get("resource_id"):
            raise DatabaseSafetyError("cloud metrics mode requires resource_id")
    else:
        if not config.get("bootstrap_servers") or not config.get("topic"):
            raise DatabaseSafetyError("CDC mode requires bootstrap_servers and topic")
        normalized["read_only"] = True
        normalized["commit_offsets"] = False
    return normalized


def bounded_int(value: Any, low: int, high: int, name: str) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise DatabaseSafetyError(f"{name} must be an integer") from exc
    if parsed < low or parsed > high:
        raise DatabaseSafetyError(f"{name} must be between {low} and {high}")
    return parsed


def ensure_select_only(statements: list[str]) -> None:
    for statement in statements:
        stripped = statement.strip().rstrip(";")
        if not stripped or WRITE_WORDS.search(stripped):
            raise DatabaseSafetyError("monitoring probes must be read-only")
        if not re.match(r"^(select|show|with|explain|pragma|db\.|rs\.|info\b)", stripped, re.I):
            raise DatabaseSafetyError("monitoring probes must use an approved read operation")


def fingerprint(text: str) -> str:
    normalized = re.sub(r"'(?:''|[^'])*'|\b\d+(?:\.\d+)?\b", "?", text)
    normalized = re.sub(r"\s+", " ", normalized).strip().lower()
    return hashlib.sha256(normalized.encode()).hexdigest()[:20]


def cdc_operation(payload: dict[str, Any]) -> str:
    """Return the change operation from common CDC event envelopes."""
    envelope = payload.get("payload")
    if not isinstance(envelope, dict):
        envelope = payload
    return str(
        envelope.get("op")
        or envelope.get("operationType")
        or payload.get("op")
        or payload.get("operationType")
        or "unknown"
    )


def normalize_metric(name: str, value: float) -> tuple[str, str]:
    text = re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", name)
    text = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", text).replace("/", "_").lower()
    aliases = {
        "cpu_utilization": "cpu_percent",
        "database_connections": "connections",
        "connections_active": "active_connections",
        "free_storage_space": "free_storage_bytes",
        "apply_lag": "replica_lag",
        "replication_replica_lag": "replica_lag",
    }
    metric = aliases.get(text, text)
    severity = "info"
    if metric in {"cpu_percent", "storage_percent"}:
        severity = "critical" if value >= 95 else "warning" if value >= 80 else "info"
    elif metric in {"replica_lag", "longest_query"}:
        severity = "critical" if value >= 60 else "warning" if value >= 10 else "info"
    elif metric in {"throttled_requests", "waiting_sessions"} and value > 0:
        severity = "warning"
    return metric, severity


class DirectReplicaConnector:
    def __init__(
        self,
        name: str,
        provider: str,
        engine: str,
        config: dict[str, Any],
        credentials: dict[str, str],
    ) -> None:
        self.name = name
        self.provider = provider
        self.engine = engine
        self.config = config
        self.credentials = credentials

    def _connect(self):
        host = str(self.config["host"])
        port = int(self.config.get("port") or default_port(self.engine))
        database = str(self.config.get("database") or "")
        username = self.credentials.get("username", "")
        password = self.credentials.get("password", "")
        timeout = int(self.config["timeout_s"])
        if self.engine == "postgresql":
            import psycopg

            return psycopg.connect(
                host=host,
                port=port,
                dbname=database,
                user=username,
                password=password,
                connect_timeout=timeout,
                options=f"-c statement_timeout={timeout * 1000} -c default_transaction_read_only=on",
                autocommit=True,
            )
        if self.engine in {"mysql", "mariadb"}:
            try:
                import pymysql
            except ImportError as exc:
                raise ConnectorUnavailable(
                    "install the database connector extras for MySQL"
                ) from exc
            return pymysql.connect(
                host=host,
                port=port,
                database=database,
                user=username,
                password=password,
                connect_timeout=timeout,
                read_timeout=timeout,
                write_timeout=timeout,
                autocommit=True,
            )
        if self.engine == "oracle":
            try:
                import oracledb
            except ImportError as exc:
                raise ConnectorUnavailable(
                    "install the database connector extras for Oracle"
                ) from exc
            return oracledb.connect(
                user=username,
                password=password,
                dsn=f"{host}:{port}/{database}",
                tcp_connect_timeout=timeout,
            )
        if self.engine == "sqlserver":
            try:
                import pyodbc
            except ImportError as exc:
                raise ConnectorUnavailable("install pyodbc and the SQL Server ODBC driver") from exc
            driver = self.config.get("odbc_driver", "ODBC Driver 18 for SQL Server")
            return pyodbc.connect(
                f"DRIVER={{{driver}}};SERVER={host},{port};DATABASE={database};"
                f"UID={username};PWD={password};Encrypt=yes;TrustServerCertificate=no;"
                f"ApplicationIntent=ReadOnly;Connection Timeout={timeout}",
                autocommit=True,
            )
        if self.engine == "mongodb":
            try:
                from pymongo import MongoClient
            except ImportError as exc:
                raise ConnectorUnavailable(
                    "install pymongo for MongoDB replica monitoring"
                ) from exc
            return MongoClient(
                host,
                port,
                username=username or None,
                password=password or None,
                authSource=self.config.get("auth_source", "admin"),
                directConnection=True,
                readPreference="secondaryPreferred",
                serverSelectionTimeoutMS=timeout * 1000,
                connectTimeoutMS=timeout * 1000,
            )
        if self.engine == "redis":
            try:
                import redis
            except ImportError as exc:
                raise ConnectorUnavailable("install redis for Redis replica monitoring") from exc
            return redis.Redis(
                host=host,
                port=port,
                username=username or None,
                password=password or None,
                db=int(self.config.get("database") or 0),
                socket_timeout=timeout,
                socket_connect_timeout=timeout,
                decode_responses=True,
            )
        raise ConnectorUnavailable(f"direct connector for {self.engine} is not available")

    def verify(self) -> dict[str, Any]:
        if self.engine == "postgresql":
            rows = self._query(
                "SELECT pg_is_in_recovery(), current_setting('transaction_read_only'), "
                "r.rolsuper FROM pg_roles r WHERE r.rolname = current_user"
            )
            is_replica, read_only, superuser = rows[0]
            if not is_replica:
                raise DatabaseSafetyError("PostgreSQL target is primary; connect a read replica")
            if str(read_only).lower() != "on" or bool(superuser):
                raise DatabaseSafetyError("PostgreSQL account must be non-superuser and read-only")
        elif self.engine in {"mysql", "mariadb"}:
            rows = self._query("SELECT @@global.read_only, @@global.super_read_only")
            if not any(bool(value) for value in rows[0]):
                raise DatabaseSafetyError("MySQL target is writable; connect a read replica")
            grants = [str(row[0]).upper() for row in self._query("SHOW GRANTS FOR CURRENT_USER")]
            forbidden = (
                " INSERT",
                " UPDATE",
                " DELETE",
                " CREATE",
                " DROP",
                " ALTER",
                " ALL PRIVILEGES",
            )
            if any(any(item in grant for item in forbidden) for grant in grants):
                raise DatabaseSafetyError("MySQL account has write privileges")
        elif self.engine == "oracle":
            role = str(self._query("SELECT database_role FROM v$database")[0][0]).upper()
            if "STANDBY" not in role:
                raise DatabaseSafetyError("Oracle target is not a standby database")
        elif self.engine == "sqlserver":
            row = self._query(
                "SELECT DATABASEPROPERTYEX(DB_NAME(), 'Updateability'), "
                "sys.fn_hadr_is_primary_replica(DB_NAME())"
            )[0]
            if str(row[0]).upper() != "READ_ONLY" and row[1] != 0:
                raise DatabaseSafetyError("SQL Server target is writable or primary")
        elif self.engine == "mongodb":
            client = self._connect()
            try:
                hello = client.admin.command("hello")
                if hello.get("isWritablePrimary") or not hello.get("secondary"):
                    raise DatabaseSafetyError("MongoDB target is not a replica-set secondary")
                status = client.admin.command("connectionStatus", showPrivileges=True)
                write_actions = {"insert", "update", "remove", "createCollection", "dropCollection"}
                actions = {
                    action
                    for privilege in status.get("authInfo", {}).get(
                        "authenticatedUserPrivileges", []
                    )
                    for action in privilege.get("actions", [])
                }
                if actions & write_actions:
                    raise DatabaseSafetyError("MongoDB account has write privileges")
            finally:
                client.close()
        elif self.engine == "redis":
            client = self._connect()
            try:
                info = client.info("replication")
                if info.get("role") not in {"slave", "replica"}:
                    raise DatabaseSafetyError("Redis target is not a replica")
                username = self.credentials.get("username")
                if username:
                    users = client.acl_getuser(username) or {}
                    commands = " ".join(users.get("commands") or [])
                    if "+@write" in commands or "+@all" in commands:
                        raise DatabaseSafetyError("Redis account permits write commands")
            finally:
                client.close()
        return {"ok": True, "replica": True, "read_only": True, "engine": self.engine}

    def discover(self) -> list[dict[str, str]]:
        if self.engine == "mongodb":
            client = self._connect()
            try:
                return [
                    {"kind": "database", "value": value} for value in client.list_database_names()
                ]
            finally:
                client.close()
        if self.engine == "redis":
            return [{"kind": "database", "value": str(self.config.get("database") or 0)}]
        query = {
            "postgresql": "SELECT datname FROM pg_database WHERE datallowconn AND NOT datistemplate ORDER BY datname",
            "mysql": "SHOW DATABASES",
            "mariadb": "SHOW DATABASES",
            "oracle": "SELECT name FROM v$database",
            "sqlserver": "SELECT name FROM sys.databases WHERE state = 0 ORDER BY name",
        }[self.engine]
        return [{"kind": "database", "value": str(row[0])} for row in self._query(query)]

    def collect(self) -> CollectionResult:
        started = time.monotonic()
        self.verify()
        signals: list[DatabaseSignal] = []
        now_ms = int(time.time() * 1000)
        database = str(self.config.get("database") or "")
        resource = str(self.config.get("resource_id") or self.config["host"])
        if self.engine == "postgresql":
            row = self._query(
                "SELECT count(*) FILTER (WHERE state = 'active'), "
                "count(*) FILTER (WHERE wait_event IS NOT NULL), "
                "coalesce(max(extract(epoch FROM now() - query_start)), 0) "
                "FROM pg_stat_activity WHERE pid <> pg_backend_pid()"
            )[0]
            metrics = [
                ("active_connections", row[0], "count"),
                ("waiting_sessions", row[1], "count"),
                ("longest_query", row[2], "seconds"),
            ]
        elif self.engine in {"mysql", "mariadb"}:
            rows = self._query(
                "SHOW GLOBAL STATUS WHERE Variable_name IN ('Threads_connected','Threads_running','Slow_queries')"
            )
            metrics = [(str(row[0]).lower(), float(row[1]), "count") for row in rows]
        elif self.engine == "oracle":
            row = self._query(
                "SELECT count(*), sum(CASE WHEN status='ACTIVE' THEN 1 ELSE 0 END) FROM v$session WHERE type='USER'"
            )[0]
            metrics = [("connections", row[0], "count"), ("active_connections", row[1], "count")]
        elif self.engine == "mongodb":
            client = self._connect()
            try:
                status = client.admin.command("serverStatus")
                connections = status.get("connections", {})
                metrics = [
                    ("connections", connections.get("current", 0), "count"),
                    ("connection_pressure", connections.get("active", 0), "count"),
                    (
                        "operation_latency",
                        status.get("opLatencies", {}).get("reads", {}).get("latency", 0),
                        "microseconds",
                    ),
                ]
            finally:
                client.close()
        elif self.engine == "redis":
            client = self._connect()
            try:
                info = client.info()
                metrics = [
                    ("connections", info.get("connected_clients", 0), "count"),
                    ("memory_used", info.get("used_memory", 0), "bytes"),
                    ("replica_lag", info.get("master_last_io_seconds_ago", 0), "seconds"),
                ]
            finally:
                client.close()
        else:
            row = self._query(
                "SELECT count(*) FROM sys.dm_exec_sessions WHERE is_user_process = 1"
            )[0]
            metrics = [("connections", row[0], "count")]
        for metric, value, unit in metrics:
            normalized_metric, severity = normalize_metric(metric, float(value or 0))
            signals.append(
                DatabaseSignal(
                    self.name,
                    self.provider,
                    self.engine,
                    resource,
                    database,
                    normalized_metric,
                    severity,
                    float(value or 0),
                    unit,
                    now_ms,
                    {"access_mode": "replica"},
                )
            )
        return CollectionResult(
            signals=signals,
            requests=2,
            rows=len(metrics),
            duration_ms=(time.monotonic() - started) * 1000,
        )

    def _query(self, statement: str) -> list[tuple]:
        ensure_select_only([statement])
        connection = self._connect()
        try:
            cursor = connection.cursor()
            try:
                if self.engine in {"mysql", "mariadb"}:
                    cursor.execute("SET SESSION TRANSACTION READ ONLY")
                    cursor.execute(
                        f"SET SESSION MAX_EXECUTION_TIME={int(self.config['timeout_s']) * 1000}"
                    )
                cursor.execute(statement)
                rows = cursor.fetchmany(int(self.config["max_rows"]))
                return [tuple(row) for row in rows]
            finally:
                cursor.close()
        finally:
            connection.close()


def default_port(engine: str) -> int:
    return {
        "postgresql": 5432,
        "mysql": 3306,
        "mariadb": 3306,
        "oracle": 1521,
        "sqlserver": 1433,
        "mongodb": 27017,
        "redis": 6379,
        "cassandra": 9042,
    }.get(engine, 0)


def connector_for(
    name: str,
    provider: str,
    engine: str,
    access_mode: str,
    config: dict[str, Any],
    credentials: dict[str, str],
) -> DatabaseConnector:
    safe = validate_source(name, provider, engine, access_mode, config)
    if access_mode == "replica":
        return DirectReplicaConnector(name, provider, engine, safe, credentials)
    if access_mode == "cloud_metrics":
        return CloudMetricsConnector(name, provider, engine, safe, credentials)
    return CDCConnector(name, provider, engine, safe, credentials)


class CloudMetricsConnector:
    METRICS: dict[str, list[str]] = {
        "aws": [
            "CPUUtilization",
            "DatabaseConnections",
            "FreeStorageSpace",
            "ReadLatency",
            "WriteLatency",
            "ReplicaLag",
            "ThrottledRequests",
        ],
        "gcp": [
            "cpu/utilization",
            "database/network/connections",
            "disk/utilization",
            "replication/replica_lag",
        ],
        "azure": [
            "cpu_percent",
            "connections_active",
            "storage_percent",
            "replication_lag",
            "throttled_requests",
        ],
        "oci": [
            "CpuUtilization",
            "CurrentLogons",
            "StorageUtilization",
            "ApplyLag",
            "ThrottledRequests",
        ],
    }

    def __init__(
        self,
        name: str,
        provider: str,
        engine: str,
        config: dict[str, Any],
        credentials: dict[str, str],
    ) -> None:
        self.name = name
        self.provider = provider
        self.engine = engine
        self.config = config
        self.credentials = credentials

    def verify(self) -> dict[str, Any]:
        if self.provider not in self.METRICS:
            raise DatabaseSafetyError("cloud metrics require AWS, GCP, Azure, or OCI")
        points = self._collect_points()
        return {
            "ok": True,
            "cloud_metrics": True,
            "provider": self.provider,
            "resource": self.config["resource_id"],
            "metrics_found": len(points),
        }

    def discover(self) -> list[dict[str, str]]:
        return [{"kind": "resource", "value": str(self.config["resource_id"])}]

    def collect(self) -> CollectionResult:
        started = time.monotonic()
        points = self._collect_points()
        now_ms = int(time.time() * 1000)
        signals = []
        for point in points[: int(self.config["max_rows"])]:
            value = float(point["value"])
            metric, inferred_severity = normalize_metric(str(point["metric"]), value)
            signals.append(
                DatabaseSignal(
                    self.name,
                    self.provider,
                    self.engine,
                    str(self.config["resource_id"]),
                    str(self.config.get("database") or ""),
                    metric,
                    point.get("severity", inferred_severity),
                    value,
                    point.get("unit", "count"),
                    int(point.get("observed_at_unix_ms") or now_ms),
                    {"access_mode": "cloud_metrics", "provider_metric": point["metric"]},
                )
            )
        return CollectionResult(
            signals,
            requests=min(len(self.METRICS[self.provider]), int(self.config["max_requests"])),
            rows=len(signals),
            duration_ms=(time.monotonic() - started) * 1000,
            estimated_cost_usd=float(self.config.get("estimated_cost_per_cycle_usd") or 0),
        )

    def _collect_points(self) -> list[dict[str, Any]]:
        injected = self.config.get("test_points")
        if isinstance(injected, list):
            return injected
        if self.provider == "aws":
            return self._aws_points()
        if self.provider == "gcp":
            return self._gcp_points()
        if self.provider == "azure":
            return self._azure_points()
        return self._oci_points()

    def _aws_points(self) -> list[dict[str, Any]]:
        try:
            import boto3
        except ImportError as exc:
            raise ConnectorUnavailable("install boto3 for AWS managed database metrics") from exc
        region = str(self.config.get("region") or "")
        client = boto3.client(
            "cloudwatch",
            region_name=region,
            aws_access_key_id=self.credentials.get("access_key_id") or None,
            aws_secret_access_key=self.credentials.get("secret_access_key") or None,
            aws_session_token=self.credentials.get("session_token") or None,
        )
        service_defaults = {
            "dynamodb": (
                "AWS/DynamoDB",
                "TableName",
                ["ConsumedReadCapacityUnits", "ThrottledRequests", "SystemErrors"],
            ),
            "documentdb": (
                "AWS/DocDB",
                "DBClusterIdentifier",
                [
                    "CPUUtilization",
                    "DatabaseConnections",
                    "FreeLocalStorage",
                    "ReadLatency",
                    "WriteLatency",
                ],
            ),
            "elasticache-redis": (
                "AWS/ElastiCache",
                "CacheClusterId",
                [
                    "CPUUtilization",
                    "CurrConnections",
                    "DatabaseMemoryUsagePercentage",
                    "ReplicationLag",
                ],
            ),
            "redshift": (
                "AWS/Redshift",
                "ClusterIdentifier",
                ["CPUUtilization", "DatabaseConnections", "HealthStatus", "ReadIOPS", "WriteIOPS"],
            ),
            "aurora-postgresql": ("AWS/RDS", "DBClusterIdentifier", self.METRICS["aws"]),
            "aurora-mysql": ("AWS/RDS", "DBClusterIdentifier", self.METRICS["aws"]),
        }
        default_namespace, default_dimension, default_metrics = service_defaults.get(
            self.engine, ("AWS/RDS", "DBInstanceIdentifier", self.METRICS["aws"])
        )
        namespace = str(self.config.get("namespace") or default_namespace)
        dimension = str(self.config.get("dimension_name") or default_dimension)
        metrics = self.config.get("metrics") or default_metrics
        end = datetime.now(timezone.utc)
        points = []
        for metric in metrics[: int(self.config["max_requests"])]:
            response = client.get_metric_statistics(
                Namespace=namespace,
                MetricName=metric,
                Dimensions=[{"Name": dimension, "Value": str(self.config["resource_id"])}],
                StartTime=end - timedelta(minutes=5),
                EndTime=end,
                Period=300,
                Statistics=["Average"],
            )
            datapoints = response.get("Datapoints") or []
            if datapoints:
                latest = max(datapoints, key=lambda item: item["Timestamp"])
                points.append(
                    {
                        "metric": metric,
                        "value": latest["Average"],
                        "unit": latest.get("Unit", "count"),
                    }
                )
        return points

    def _gcp_points(self) -> list[dict[str, Any]]:
        try:
            from google.cloud import monitoring_v3
            from google.oauth2 import service_account
        except ImportError as exc:
            raise ConnectorUnavailable("install google-cloud-monitoring for GCP metrics") from exc
        credentials = None
        if self.credentials.get("service_account_json"):
            credentials = service_account.Credentials.from_service_account_info(
                json.loads(self.credentials["service_account_json"])
            )
        client = monitoring_v3.MetricServiceClient(credentials=credentials)
        project = str(self.config.get("project_id") or "")
        if not project:
            raise DatabaseSafetyError("GCP cloud metrics require project_id")
        interval = monitoring_v3.TimeInterval(
            end_time={"seconds": int(time.time())}, start_time={"seconds": int(time.time()) - 300}
        )
        resource_id = str(self.config["resource_id"])
        points: list[dict[str, Any]] = []
        service_defaults = {
            "alloydb": (
                "alloydb.googleapis.com/database/",
                "instance_id",
                ["cpu/utilization", "memory/quota", "postgresql/num_backends"],
            ),
            "spanner": (
                "spanner.googleapis.com/instance/",
                "instance_id",
                ["cpu/utilization_by_priority", "node_count", "storage/used_bytes"],
            ),
            "firestore": (
                "firestore.googleapis.com/document/",
                "database_id",
                ["read_count", "write_count", "delete_count"],
            ),
            "memorystore-redis": (
                "redis.googleapis.com/stats/",
                "instance_id",
                ["cpu_utilization", "memory/usage_ratio", "clients/connected"],
            ),
        }
        default_prefix, default_label, default_metrics = service_defaults.get(
            self.engine,
            ("cloudsql.googleapis.com/database/", "database_id", self.METRICS["gcp"]),
        )
        prefix = str(self.config.get("metric_prefix") or default_prefix)
        resource_label = str(self.config.get("resource_label") or default_label)
        metrics = self.config.get("metrics") or default_metrics
        for metric in metrics[: int(self.config["max_requests"])]:
            series = client.list_time_series(
                request={
                    "name": f"projects/{project}",
                    "filter": f'metric.type="{prefix}{metric}" AND resource.labels.{resource_label}="{resource_id}"',
                    "interval": interval,
                    "view": monitoring_v3.ListTimeSeriesRequest.TimeSeriesView.FULL,
                }
            )
            for item in series:
                if not item.points:
                    continue
                value = item.points[0].value
                number = value.double_value or value.int64_value
                points.append(
                    {"metric": metric, "value": number, "unit": item.metric_kind.name.lower()}
                )
                break
        return points

    def _azure_points(self) -> list[dict[str, Any]]:
        try:
            from azure.identity import ClientSecretCredential, DefaultAzureCredential
            from azure.monitor.query import MetricsQueryClient
        except ImportError as exc:
            raise ConnectorUnavailable(
                "install azure-identity and azure-monitor-query for Azure metrics"
            ) from exc
        if self.credentials.get("client_secret"):
            credential = ClientSecretCredential(
                tenant_id=self.credentials.get("tenant_id", ""),
                client_id=self.credentials.get("client_id", ""),
                client_secret=self.credentials["client_secret"],
            )
        else:
            credential = DefaultAzureCredential()
        client = MetricsQueryClient(credential)
        names = self.METRICS["azure"][: int(self.config["max_requests"])]
        response = client.query_resource(
            str(self.config["resource_id"]), metric_names=names, timespan=timedelta(minutes=5)
        )
        points: list[dict[str, Any]] = []
        for metric in response.metrics:
            values = [
                point.average
                for series in metric.timeseries
                for point in series.data
                if point.average is not None
            ]
            if values:
                points.append(
                    {"metric": metric.name, "value": values[-1], "unit": str(metric.unit)}
                )
        return points

    def _oci_points(self) -> list[dict[str, Any]]:
        try:
            import oci
        except ImportError as exc:
            raise ConnectorUnavailable("install oci for Oracle Cloud metrics") from exc
        if self.credentials.get("config_json"):
            oci_config = json.loads(self.credentials["config_json"])
        else:
            oci_config = oci.config.from_file(
                file_location=self.config.get("oci_config_file", oci.config.DEFAULT_LOCATION),
                profile_name=self.config.get("oci_profile", "DEFAULT"),
            )
        client = oci.monitoring.MonitoringClient(oci_config)
        compartment = str(self.config.get("compartment_id") or "")
        namespace = str(self.config.get("namespace") or "oci_database")
        if not compartment:
            raise DatabaseSafetyError("OCI cloud metrics require compartment_id")
        points: list[dict[str, Any]] = []
        for metric in self.METRICS["oci"][: int(self.config["max_requests"])]:
            query = f'{metric}[5m]{{resourceId = "{self.config["resource_id"]}"}}.mean()'
            response = client.summarize_metrics_data(
                compartment,
                oci.monitoring.models.SummarizeMetricsDataDetails(namespace=namespace, query=query),
            )
            for item in response.data:
                if item.aggregated_datapoints:
                    latest = item.aggregated_datapoints[-1]
                    points.append({"metric": metric, "value": latest.value, "unit": "count"})
                    break
        return points


class CDCConnector:
    def __init__(
        self,
        name: str,
        provider: str,
        engine: str,
        config: dict[str, Any],
        credentials: dict[str, str],
    ) -> None:
        self.name = name
        self.provider = provider
        self.engine = engine
        self.config = config
        self.credentials = credentials

    def verify(self) -> dict[str, Any]:
        try:
            from confluent_kafka import Consumer
        except ImportError as exc:
            raise ConnectorUnavailable("confluent-kafka is required for CDC monitoring") from exc
        consumer = Consumer(self._settings())
        try:
            metadata = consumer.list_topics(
                topic=self.config["topic"], timeout=int(self.config["timeout_s"])
            )
            topic = metadata.topics.get(self.config["topic"])
            if topic is None or topic.error is not None:
                raise ConnectorUnavailable("CDC topic was not found or is not readable")
            partitions = len(topic.partitions)
        finally:
            consumer.close()
        return {
            "ok": True,
            "read_only": True,
            "commit_offsets": False,
            "topic": self.config["topic"],
            "partitions": partitions,
        }

    def discover(self) -> list[dict[str, str]]:
        return [{"kind": "topic", "value": str(self.config["topic"])}]

    def collect(self) -> CollectionResult:
        try:
            from confluent_kafka import Consumer, KafkaException
        except ImportError as exc:
            raise ConnectorUnavailable("confluent-kafka is required for CDC monitoring") from exc
        consumer = Consumer(self._settings())
        started = time.monotonic()
        counts: dict[str, int] = {}
        rows = 0
        try:
            consumer.subscribe([self.config["topic"]])
            deadline = time.monotonic() + min(int(self.config["max_cycle_s"]), 10)
            while rows < int(self.config["max_rows"]) and time.monotonic() < deadline:
                message = consumer.poll(0.25)
                if message is None:
                    continue
                if message.error():
                    raise KafkaException(message.error())
                rows += 1
                try:
                    payload = json.loads(message.value() or b"{}")
                except json.JSONDecodeError:
                    payload = {}
                operation = cdc_operation(payload)
                counts[operation] = counts.get(operation, 0) + 1
        finally:
            consumer.close()
        now_ms = int(time.time() * 1000)
        signals = [
            DatabaseSignal(
                self.name,
                self.provider,
                self.engine,
                str(self.config["topic"]),
                str(self.config.get("database") or ""),
                "cdc_change_rate",
                "info",
                float(count),
                "events",
                now_ms,
                {"operation": operation, "access_mode": "cdc", "offsets_committed": False},
            )
            for operation, count in sorted(counts.items())
        ]
        return CollectionResult(
            signals,
            requests=rows,
            rows=rows,
            duration_ms=(time.monotonic() - started) * 1000,
        )

    def _settings(self) -> dict[str, Any]:
        settings: dict[str, Any] = {
            "bootstrap.servers": self.config["bootstrap_servers"],
            "group.id": self.config.get("consumer_group", f"quellops-db-{self.name}"),
            "enable.auto.commit": False,
            "auto.offset.reset": "latest",
            "socket.timeout.ms": int(self.config["timeout_s"]) * 1000,
        }
        if self.credentials.get("sasl_username"):
            settings.update(
                {
                    "security.protocol": self.config.get("security_protocol", "SASL_SSL"),
                    "sasl.mechanism": self.config.get("sasl_mechanism", "PLAIN"),
                    "sasl.username": self.credentials["sasl_username"],
                    "sasl.password": self.credentials.get("sasl_password", ""),
                }
            )
        return settings
