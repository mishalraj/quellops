from __future__ import annotations

import hmac
import json
import re
import threading
from contextlib import asynccontextmanager

from fastapi import APIRouter, Header, HTTPException, Query
from pydantic import BaseModel, Field

from .engine import LogSignal, SignalStore
from .log_analysis import LogSignalAnalyzer
from .log_client import LogClient, LogClientError
from .log_store import create_log_source_store
from .log_watcher import LogWatcher


COMMON_FIELDS = [
    {"name": "redact_patterns", "label": "Sensitive data redaction", "type": "list", "advanced": True},
    {"name": "initial_window_s", "label": "Initial window (seconds)", "type": "number", "default": 60, "advanced": True},
    {"name": "initial_records", "label": "Initial record limit", "type": "number", "default": 50, "advanced": True},
    {"name": "target_records", "label": "Target records", "type": "number", "default": 100, "advanced": True},
    {"name": "max_window_s", "label": "Maximum request window (seconds)", "type": "number", "default": 900, "advanced": True},
    {"name": "max_scope_s", "label": "Maximum investigation scope (seconds)", "type": "number", "default": 3600, "advanced": True},
    {"name": "max_records", "label": "Maximum records per request", "type": "number", "default": 500, "advanced": True},
    {"name": "max_bytes", "label": "Maximum bytes per request", "type": "number", "default": 2097152, "advanced": True},
    {"name": "timeout_s", "label": "Request timeout (seconds)", "type": "number", "default": 15, "advanced": True},
    {"name": "max_pages", "label": "Maximum pages per request", "type": "number", "default": 5, "advanced": True},
    {"name": "growth_factor", "label": "Adaptive growth factor", "type": "number", "default": 2, "advanced": True},
    {"name": "min_split_window_s", "label": "Minimum split window (seconds)", "type": "number", "default": 5, "advanced": True},
    {"name": "max_cycle_fetches", "label": "Maximum fetches per cycle", "type": "number", "default": 20, "advanced": True},
    {"name": "max_cycle_requests", "label": "Maximum provider requests per cycle", "type": "number", "default": 50, "advanced": True},
    {"name": "max_cycle_bytes", "label": "Maximum bytes per cycle", "type": "number", "default": 16777216, "advanced": True},
    {"name": "max_cycle_records", "label": "Maximum records per cycle", "type": "number", "default": 5000, "advanced": True},
    {"name": "max_cycle_seconds", "label": "Maximum cycle time (seconds)", "type": "number", "default": 60, "advanced": True},
]


def field(name: str, label: str, *, required: bool = False, kind: str = "text", default=None) -> dict:
    result = {"name": name, "label": label, "type": kind, "required": required}
    if default is not None:
        result["default"] = default
    return result


def secret(name: str, label: str) -> dict:
    return {"name": name, "label": label}


PROVIDERS = {
    "loki": {
        "label": "Grafana Loki",
        "fields": [field("url", "Loki URL", required=True)],
        "credentials": [secret("token", "Bearer token"), secret("username", "Basic auth username / instance ID"), secret("password", "Basic auth password / access token"), secret("header:X-Scope-OrgID", "Tenant ID")],
    },
    "splunk": {
        "label": "Splunk",
        "fields": [field("url", "Splunk management URL", required=True)],
        "credentials": [secret("token", "Splunk token"), secret("username", "Username"), secret("password", "Password")],
    },
    "elasticsearch": {
        "label": "Elasticsearch",
        "fields": [field("url", "Elasticsearch URL", required=True), field("index", "Index", required=True, default="logs-*"), field("timestamp_field", "Timestamp field", required=True, default="@timestamp")],
        "credentials": [secret("token", "Bearer token"), secret("username", "Username"), secret("password", "Password"), secret("header:Authorization", "Authorization header (for example, ApiKey …)")],
    },
    "opensearch": {
        "label": "OpenSearch",
        "fields": [field("url", "OpenSearch URL", required=True), field("index", "Index", required=True, default="logs-*"), field("timestamp_field", "Timestamp field", required=True, default="@timestamp"), field("aws_sigv4", "Use AWS SigV4", kind="boolean"), field("region", "AWS region"), field("aws_service", "AWS service", default="es")],
        "credentials": [secret("token", "Bearer token"), secret("username", "Username"), secret("password", "Password"), secret("header:Authorization", "Authorization header"), secret("aws_access_key_id", "AWS access key ID"), secret("aws_secret_access_key", "AWS secret access key"), secret("aws_session_token", "AWS session token")],
    },
    "graylog": {
        "label": "Graylog",
        "fields": [field("url", "Graylog API URL", required=True), field("graylog_api", "API mode", required=True, default="scripting")],
        "credentials": [secret("token", "Bearer token"), secret("username", "Username or token"), secret("password", "Password or token secret")],
    },
    "datadog": {
        "label": "Datadog",
        "fields": [field("url", "Datadog API URL", required=True, default="https://api.datadoghq.com")],
        "credentials": [secret("api_key", "API key"), secret("app_key", "Application key")],
    },
    "cloudwatch": {
        "label": "AWS CloudWatch Logs",
        "fields": [field("region", "AWS region", required=True)],
        "credentials": [secret("aws_access_key_id", "AWS access key ID"), secret("aws_secret_access_key", "AWS secret access key"), secret("aws_session_token", "AWS session token")],
    },
    "gcp": {
        "label": "Google Cloud Logging",
        "fields": [field("url", "Logging API URL", required=True, default="https://logging.googleapis.com"), field("resource_names", "Resource names", required=True, kind="list")],
        "credentials": [secret("token", "Access token")],
    },
    "azure": {
        "label": "Azure Log Analytics",
        "fields": [field("url", "Log Analytics API URL", required=True, default="https://api.loganalytics.io"), field("workspace_id", "Workspace ID", required=True), field("timestamp_field", "Timestamp field", default="TimeGenerated"), field("message_field", "Message field", default="Message")],
        "credentials": [secret("token", "Access token")],
    },
}

SOURCE_NAME = re.compile(r"^[A-Za-z0-9_.-]+$")
# query and log_group remain accepted for existing saved configurations and API
# clients, but the normal UI obtains them through provider discovery.
CONTROL_FIELDS = {
    "scope_key", "scope_value", "scope_values", "custom_message", "custom_level",
    "query", "log_group", "log_groups",
}


class SourceInput(BaseModel):
    provider: str
    service: str = Field(default="", max_length=128)
    config: dict = Field(default_factory=dict)
    credentials: dict[str, str | None] = Field(default_factory=dict)


class WatcherInput(BaseModel):
    enabled: bool = False
    interval_s: int = Field(default=30, ge=5, le=3600)
    lag_s: int = Field(default=5, ge=0, le=300)
    initial_lookback_s: int = Field(default=60, ge=1, le=86400)


def router(
    target: str,
    service_token: str,
    admin_token: str,
    master_key: str,
    db_path: str,
    signals: SignalStore | None = None,
    database_url: str = "",
) -> APIRouter:
    enabled = bool(
        target and service_token and admin_token and master_key and (db_path or database_url)
    )
    client = LogClient(target, service_token) if enabled else None
    store = (
        create_log_source_store(db_path, master_key, database_url)
        if enabled
        else None
    )
    analyzer = (
        LogSignalAnalyzer(signals, client, history_store=store)
        if signals is not None and client is not None and store is not None
        else None
    )
    watcher = (
        LogWatcher(store, client, analyzer.observe if analyzer is not None else None)
        if store is not None and client is not None
        else None
    )
    state = {"sync_error": None}
    reconcile_stop = threading.Event()
    control_lock = threading.Lock()

    def reconcile() -> None:
        if store is None or client is None:
            return
        with control_lock:
            errors = []
            try:
                client.list_sources()
            except LogClientError as exc:
                state["sync_error"] = str(exc)
                return
            for source in store.list():
                saved = store.get(source["name"])
                if saved is None:
                    continue
                try:
                    client.apply_source(
                        saved["name"],
                        saved["provider"],
                        saved["config"],
                        saved["credentials"],
                    )
                except LogClientError as exc:
                    errors.append(f"{saved['name']}: {exc}")
            state["sync_error"] = "; ".join(errors) or None

    def reconcile_loop() -> None:
        while not reconcile_stop.wait(10):
            reconcile()

    def restore_detection_history() -> None:
        if store is None or signals is None:
            return
        # Rehydrate recent evidence for incident correlation after a process restart.
        for item in reversed(store.list_detections(limit=1000)):
            try:
                signals.add_log_signal(LogSignal(**item))
            except (TypeError, ValueError):
                continue

    @asynccontextmanager
    async def lifespan(_app):
        if store is not None and client is not None:
            store.initialize()
            restore_detection_history()
            reconcile()
            reconcile_stop.clear()
            reconcile_thread = threading.Thread(
                target=reconcile_loop, name="aidbg-log-reconciler", daemon=True
            )
            reconcile_thread.start()
            watcher.start()
        else:
            reconcile_thread = None
        yield
        reconcile_stop.set()
        if reconcile_thread is not None:
            reconcile_thread.join(timeout=5)
        if watcher is not None:
            watcher.stop()
        if client is not None:
            client.close()

    routes = APIRouter(prefix="/admin", tags=["log administration"], lifespan=lifespan)

    def authorize(authorization: str | None) -> None:
        if not enabled:
            raise HTTPException(503, "Log administration is not configured")
        expected = "Bearer " + admin_token
        if not authorization or not hmac.compare_digest(authorization, expected):
            raise HTTPException(401, "Administrator authentication required")

    def call(operation):
        try:
            return operation()
        except LogClientError as exc:
            raise HTTPException(503 if exc.unavailable else 400, str(exc)) from None

    def validate(name: str, body: SourceInput) -> tuple[dict, dict[str, str]]:
        if not SOURCE_NAME.fullmatch(name):
            raise HTTPException(422, "Source name may contain letters, numbers, dot, dash and underscore")
        schema = PROVIDERS.get(body.provider)
        if schema is None:
            raise HTTPException(422, "Unsupported log provider")
        allowed_fields = {item["name"] for item in schema["fields"] + COMMON_FIELDS} | CONTROL_FIELDS
        unknown_fields = sorted(set(body.config) - allowed_fields)
        if unknown_fields:
            raise HTTPException(422, "Unknown configuration fields: " + ", ".join(unknown_fields))
        config = {key: value for key, value in body.config.items() if value not in (None, "")}
        if len(json.dumps(config)) > 131072:
            raise HTTPException(422, "Source configuration is too large")
        for item in schema["fields"]:
            if item.get("required") and config.get(item["name"]) in (None, "", []):
                raise HTTPException(422, item["label"] + " is required")
        allowed_credentials = {item["name"] for item in schema["credentials"]}
        unknown_credentials = sorted(set(body.credentials) - allowed_credentials)
        if unknown_credentials:
            raise HTTPException(422, "Unknown credential fields: " + ", ".join(unknown_credentials))
        existing = store.get(name) if store is not None else None
        credentials = {
            key: value
            for key, value in (existing or {}).get("credentials", {}).items()
            if key in allowed_credentials
        }
        for key, value in body.credentials.items():
            if value is None:
                credentials.pop(key, None)
            elif value:
                if len(value) > 16384:
                    raise HTTPException(422, "Credential value is too long")
                credentials[key] = value
        return config, credentials

    @routes.get("/log-schemas")
    def schemas(authorization: str | None = Header(default=None)):
        # Provider names and field descriptions are public UI metadata. Keeping
        # this readable before admin authentication lets the setup form work
        # even when the token dialog is cancelled.
        return {"providers": PROVIDERS, "common_fields": COMMON_FIELDS, "sync_error": state["sync_error"]}

    @routes.get("/log-sources")
    def sources(authorization: str | None = Header(default=None)):
        authorize(authorization)
        return {"sources": store.list(), "sync_error": state["sync_error"]}

    @routes.put("/log-sources/{name}")
    def upsert(name: str, body: SourceInput, authorization: str | None = Header(default=None)):
        authorize(authorization)
        with control_lock:
            config, credentials = validate(name, body)
            old = store.get(name)
            call(lambda: client.apply_source(name, body.provider, config, credentials))
            try:
                saved = store.upsert(
                    name, body.provider, config, credentials, service=body.service.strip()
                )
                if old is not None and (
                    old["provider"] != body.provider or old["config"] != config
                ):
                    store.reset_watcher_checkpoint(name)
            except Exception:
                if old is None:
                    call(lambda: client.delete_source(name))
                else:
                    call(lambda: client.apply_source(name, old["provider"], old["config"], old["credentials"]))
                raise
        state["sync_error"] = None
        return saved

    @routes.post("/log-sources/{name}/test")
    def test(name: str, authorization: str | None = Header(default=None)):
        authorize(authorization)
        if store.get(name) is None:
            raise HTTPException(404, "Log source not found")
        return call(lambda: client.test_source(name))

    @routes.post("/log-sources/{name}/discover")
    def discover(name: str, authorization: str | None = Header(default=None)):
        authorize(authorization)
        if store.get(name) is None:
            raise HTTPException(404, "Log source not found")
        return call(lambda: client.discover(name))

    @routes.delete("/log-sources/{name}")
    def delete(name: str, authorization: str | None = Header(default=None)):
        authorize(authorization)
        with control_lock:
            if store.get(name) is None:
                raise HTTPException(404, "Log source not found")
            call(lambda: client.delete_source(name))
            return {"deleted": store.delete(name)}

    @routes.get("/log-watchers")
    def watchers(authorization: str | None = Header(default=None)):
        authorize(authorization)
        return {"watchers": store.list_watchers()}

    @routes.get("/log-monitoring-status")
    def monitoring_status(authorization: str | None = Header(default=None)):
        authorize(authorization)
        watchers_by_source = {
            item["source_name"]: item for item in store.list_watchers()
        }
        latest_signals: dict[str, dict] = {}
        latest_investigations: dict[str, dict] = {}
        if store is not None:
            for item in store.list_detections(limit=300):
                source_name = str(item.get("source") or "")
                if source_name and source_name not in latest_signals:
                    latest_signals[source_name] = item
                investigation = item.get("investigation") or {}
                if (
                    source_name
                    and source_name not in latest_investigations
                    and (investigation.get("focus") or investigation.get("error"))
                ):
                    latest_investigations[source_name] = investigation
        result = []
        for source in store.list():
            watcher_state = watchers_by_source.get(source["name"])
            config = source.get("config") or {}
            signal = latest_signals.get(source["name"])
            if watcher_state and watcher_state.get("last_error"):
                status_name = "error"
            elif watcher_state and watcher_state.get("enabled"):
                status_name = "monitoring"
            elif config.get("scope_values") or config.get("scope_value") or config.get("query") or config.get("log_group"):
                status_name = "paused"
            else:
                status_name = "needs_scope"
            result.append(
                {
                    "source": source["name"],
                    "provider": source["provider"],
                    "service": source.get("service") or "",
                    "scope_kind": config.get("scope_key") or "",
                    "scope_value": ", ".join(config.get("scope_values") or []) or config.get("scope_value") or config.get("log_group") or "",
                    "status": status_name,
                    "watcher": watcher_state,
                    "latest_detection": signal,
                    "latest_investigation": latest_investigations.get(source["name"]),
                }
            )
        return {"statuses": result}

    @routes.get("/log-detections")
    def detections(
        source: str | None = None,
        limit: int = Query(default=100, ge=1, le=1000),
        authorization: str | None = Header(default=None),
    ):
        authorize(authorization)
        return {"detections": store.list_detections(source, limit=limit)}

    @routes.put("/log-sources/{name}/watcher")
    def configure_watcher(
        name: str,
        body: WatcherInput,
        authorization: str | None = Header(default=None),
    ):
        authorize(authorization)
        saved = store.get(name)
        if saved is None:
            raise HTTPException(404, "Log source not found")
        if body.enabled and not (saved["config"].get("scope_values") or saved["config"].get("scope_value") or saved["config"].get("query") or saved["config"].get("log_group")):
            raise HTTPException(422, "Discover and choose a monitoring scope before enabling automatic monitoring")
        return store.upsert_watcher(
            name,
            enabled=body.enabled,
            interval_s=body.interval_s,
            lag_s=body.lag_s,
            initial_lookback_s=body.initial_lookback_s,
        )

    @routes.post("/log-sources/{name}/watcher/run")
    def run_watcher(name: str, authorization: str | None = Header(default=None)):
        authorize(authorization)
        if store.get_watcher(name) is None:
            raise HTTPException(404, "Log watcher not configured")
        return watcher.run_once(name)

    return routes
