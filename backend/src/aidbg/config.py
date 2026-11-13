from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


def _env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class Settings:
    # OTLP gRPC receiver bind (collector exports here)
    otlp_grpc_bind: str = os.getenv("AIDBG_OTLP_GRPC_BIND", "0.0.0.0:4317")
    # Minimal HTTP API bind (health + basic stats)
    http_bind: str = os.getenv("AIDBG_HTTP_BIND", "0.0.0.0:8088")
    # Standalone Go logs tool. The backend contains no provider-specific code.
    log_service_grpc_target: str = os.getenv("AIDBG_LOG_SERVICE_GRPC", "")
    log_service_token: str = os.getenv("AIDBG_LOG_SERVICE_TOKEN", "")
    log_tls_ca: str = os.getenv("AIDBG_LOG_TLS_CA", "")
    log_tls_server_name: str = os.getenv("AIDBG_LOG_TLS_SERVER_NAME", "")
    log_api_token: str = os.getenv("AIDBG_LOG_API_TOKEN", "")
    log_admin_token: str = os.getenv("AIDBG_ADMIN_API_TOKEN", "")
    log_master_key: str = os.getenv("AIDBG_MASTER_KEY", "")
    change_ingest_token: str = os.getenv("AIDBG_CHANGE_INGEST_TOKEN", "")
    github_webhook_secret: str = os.getenv("AIDBG_GITHUB_WEBHOOK_SECRET", "")
    control_db_path: str = os.getenv(
        "AIDBG_CONTROL_DB",
        str(Path(__file__).resolve().parents[2] / "data" / "aidbg-control.db"),
    )
    # Set this to use shared PostgreSQL storage instead of the local SQLite file.
    control_database_url: str = os.getenv("AIDBG_CONTROL_DATABASE_URL", "")
    # Shared bootstrap credential used by outbound Kubernetes agents.
    k8s_agent_token: str = os.getenv("AIDBG_K8S_AGENT_TOKEN", "")
    k8s_retention_events: int = int(os.getenv("AIDBG_K8S_RETENTION_EVENTS", "5000"))
    incident_retention: int = int(os.getenv("AIDBG_INCIDENT_RETENTION", "500"))
    database_signal_retention: int = int(os.getenv("AIDBG_DATABASE_SIGNAL_RETENTION", "5000"))
    # Optional Kafka-compatible event stream. Database mode remains zero-setup.
    event_transport: str = os.getenv("AIDBG_EVENT_TRANSPORT", "database")
    kafka_bootstrap_servers: str = os.getenv("AIDBG_KAFKA_BOOTSTRAP_SERVERS", "")
    kafka_k8s_topic: str = os.getenv("AIDBG_KAFKA_K8S_TOPIC", "quellops.kubernetes.events.v1")
    kafka_consumer_group: str = os.getenv("AIDBG_KAFKA_CONSUMER_GROUP", "quellops-central-v1")
    # Local development may embed the consumer. Production runs aidbg.kafka_worker.
    kafka_consumer_embedded: bool = _env_bool("AIDBG_KAFKA_CONSUMER_EMBEDDED", True)
    kafka_security_protocol: str = os.getenv("AIDBG_KAFKA_SECURITY_PROTOCOL", "")
    kafka_sasl_mechanism: str = os.getenv("AIDBG_KAFKA_SASL_MECHANISM", "")
    kafka_sasl_username: str = os.getenv("AIDBG_KAFKA_SASL_USERNAME", "")
    kafka_sasl_password: str = os.getenv("AIDBG_KAFKA_SASL_PASSWORD", "")
    kafka_ssl_ca_location: str = os.getenv("AIDBG_KAFKA_SSL_CA_LOCATION", "")
    # Analysis windows in seconds (short windows for local MVP).
    current_window_s: int = int(os.getenv("AIDBG_CURRENT_WINDOW_S", "300"))
    baseline_window_s: int = int(os.getenv("AIDBG_BASELINE_WINDOW_S", "900"))
    # Optional Azure OpenAI config for explanation layer.
    azure_openai_endpoint: str = os.getenv("AZURE_OPENAI_ENDPOINT", "")
    azure_openai_api_key: str = os.getenv("AZURE_OPENAI_API_KEY", "")
    azure_openai_api_version: str = os.getenv("AZURE_OPENAI_API_VERSION", "2024-12-01-preview")
    azure_openai_deployment: str = os.getenv("AZURE_OPENAI_DEPLOYMENT", "")
    service_map_path: str = os.getenv(
        "AIDBG_SERVICE_MAP_PATH",
        str(Path(__file__).resolve().parents[2] / "service_map.yaml"),
    )
    change_horizon_s: int = int(os.getenv("AIDBG_CHANGE_HORIZON_S", "900"))
    db_slow_query_threshold_ms: float = float(os.getenv("AIDBG_DB_SLOW_MS", "100"))


def load_settings() -> Settings:
    return Settings()
