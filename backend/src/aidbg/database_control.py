from __future__ import annotations

import hmac
import json
from contextlib import asynccontextmanager

from fastapi import APIRouter, Header, HTTPException
from pydantic import BaseModel, Field

from .database_connectors import (
    ENGINE_CATALOG,
    PROVIDER_CATALOG,
    DatabaseSafetyError,
    connector_for,
    validate_source,
)
from .database_store import DatabaseStore, create_database_store
from .database_watcher import DatabaseWatcher
from .engine import ChangeEvent, SignalStore


class SourceInput(BaseModel):
    name: str = Field(min_length=1, max_length=160)
    provider: str
    engine: str
    access_mode: str
    service: str = Field(default="", max_length=160)
    config: dict = Field(default_factory=dict)
    credentials: dict[str, str | None] = Field(default_factory=dict)


class WatcherInput(BaseModel):
    enabled: bool = False
    interval_s: int = Field(default=60, ge=15, le=86400)


def router(
    admin_token: str,
    master_key: str,
    db_path: str,
    signals: SignalStore,
    retention: int = 5000,
    database_url: str = "",
) -> APIRouter:
    enabled = bool(admin_token and master_key and (db_path or database_url))
    store = create_database_store(db_path, master_key, database_url) if enabled else None

    def observe(signal: dict) -> None:
        signals.add_change_event(
            ChangeEvent(
                kind=f"db.{signal['signal']}",
                at_unix_ms=int(signal["observed_at_unix_ms"]),
                entity=str(signal.get("resource") or signal["source"]),
                namespace=signal.get("database") or None,
                metadata={
                    "source": signal["source"], "provider": signal["provider"],
                    "engine": signal["engine"], "severity": signal["severity"],
                    "value": signal["value"], "unit": signal["unit"],
                    "evidence": signal.get("evidence", {}),
                },
            )
        )

    watcher = DatabaseWatcher(store, observe, retention) if store is not None else None

    @asynccontextmanager
    async def lifespan(_app):
        if store is not None and watcher is not None:
            store.initialize()
            for item in reversed(store.list_signals(limit=min(retention, 1000))):
                observe(item)
            watcher.start()
        yield
        if watcher is not None:
            watcher.stop()

    result = APIRouter(prefix="/admin/databases", tags=["database"], lifespan=lifespan)

    def authorize(value: str | None) -> None:
        if not enabled:
            raise HTTPException(503, "Database module is not configured")
        if not value or not hmac.compare_digest(value, "Bearer " + admin_token):
            raise HTTPException(401, "Administrator authentication required")

    def required_store() -> DatabaseStore:
        if store is None:
            raise HTTPException(503, "Database module is not configured")
        return store

    def build(payload: SourceInput, credentials: dict[str, str] | None = None):
        if len(json.dumps(payload.config)) > 65_536:
            raise HTTPException(413, "database configuration is too large")
        if len(json.dumps(payload.credentials)) > 65_536:
            raise HTTPException(413, "database credentials are too large")
        if credentials is not None and len(json.dumps(credentials)) > 65_536:
            raise HTTPException(413, "stored database credentials are too large")
        try:
            safe = validate_source(
                payload.name, payload.provider, payload.engine, payload.access_mode, payload.config
            )
            return connector_for(
                payload.name, payload.provider, payload.engine, payload.access_mode,
                safe, credentials if credentials is not None else {
                    key: value for key, value in payload.credentials.items() if value is not None
                },
            ), safe
        except DatabaseSafetyError as exc:
            raise HTTPException(400, str(exc)) from exc

    @result.get("/catalog")
    def catalog(authorization: str | None = Header(default=None)):
        authorize(authorization)
        return {"providers": PROVIDER_CATALOG, "engines": ENGINE_CATALOG, "access_modes": {
            "cloud_metrics": "Provider monitoring APIs; no database query",
            "replica": "Read-only health queries against a verified replica",
            "cdc": "Observe an existing CDC topic without committing offsets",
        }}

    @result.get("/sources")
    def list_sources(authorization: str | None = Header(default=None)):
        authorize(authorization)
        return {"sources": required_store().list_sources(), "watchers": required_store().list_watchers()}

    @result.put("/sources/{name}")
    def save_source(name: str, payload: SourceInput, authorization: str | None = Header(default=None)):
        authorize(authorization)
        if name != payload.name:
            raise HTTPException(400, "path and source name must match")
        existing = required_store().get_source(name)
        credentials = dict((existing or {}).get("credentials", {}))
        credentials.update({key: value for key, value in payload.credentials.items() if value is not None})
        _, safe = build(payload, credentials)
        return required_store().upsert_source(
            name, payload.provider, payload.engine, payload.access_mode, payload.service,
            safe, credentials,
        )

    @result.delete("/sources/{name}")
    def delete_source(name: str, authorization: str | None = Header(default=None)):
        authorize(authorization)
        if not required_store().delete_source(name):
            raise HTTPException(404, "database source was not found")
        return {"ok": True}

    def saved_connector(name: str):
        source = required_store().get_source(name)
        if source is None:
            raise HTTPException(404, "database source was not found")
        try:
            return connector_for(
                source["name"], source["provider"], source["engine"],
                source["access_mode"], source["config"], source["credentials"],
            )
        except DatabaseSafetyError as exc:
            raise HTTPException(400, str(exc)) from exc

    @result.post("/sources/{name}/test")
    def test_source(name: str, authorization: str | None = Header(default=None)):
        authorize(authorization)
        try:
            return saved_connector(name).verify()
        except Exception as exc:
            raise HTTPException(400, str(exc)) from exc

    @result.get("/sources/{name}/discover")
    def discover(name: str, authorization: str | None = Header(default=None)):
        authorize(authorization)
        try:
            return {"items": saved_connector(name).discover()}
        except Exception as exc:
            raise HTTPException(400, str(exc)) from exc

    @result.put("/sources/{name}/watcher")
    def configure_watcher(name: str, payload: WatcherInput, authorization: str | None = Header(default=None)):
        authorize(authorization)
        if required_store().get_source(name) is None:
            raise HTTPException(404, "database source was not found")
        return required_store().upsert_watcher(name, payload.enabled, payload.interval_s)

    @result.post("/sources/{name}/run")
    def run_source(name: str, authorization: str | None = Header(default=None)):
        authorize(authorization)
        if watcher is None:
            raise HTTPException(503, "Database module is not configured")
        response = watcher.run_once(name)
        if not response["ok"]:
            raise HTTPException(400, response["error"])
        return response

    @result.get("/signals")
    def recent_signals(source: str | None = None, limit: int = 100, authorization: str | None = Header(default=None)):
        authorize(authorization)
        return {"signals": required_store().list_signals(source, max(1, min(limit, 1000)))}

    return result
