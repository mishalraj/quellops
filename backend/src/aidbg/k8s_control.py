from __future__ import annotations

import hmac
import re
import threading
import time
from contextlib import asynccontextmanager

from fastapi import APIRouter, Header, HTTPException, Query
from pydantic import BaseModel, ConfigDict, Field, field_validator

from .engine import ChangeEvent, K8sSignal, SignalStore
from .k8s_store import create_kubernetes_store


CLUSTER_ID = re.compile(r"^[A-Za-z0-9_.:-]{3,160}$")
NAMESPACE = re.compile(r"^[a-z0-9]([-a-z0-9.]*[a-z0-9])?$")


class WorkloadInput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    namespace: str = Field(max_length=253)
    name: str = Field(max_length=253)
    kind: str = Field(max_length=64)


class RegistrationInput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    cluster_id: str = Field(min_length=3, max_length=160)
    name: str = Field(min_length=1, max_length=160)
    agent_version: str = Field(default="", max_length=64)
    kubernetes_version: str = Field(default="", max_length=64)
    namespaces: list[str] = Field(default_factory=list, max_length=2000)
    workloads: list[WorkloadInput] = Field(default_factory=list, max_length=10000)
    queue_depth: int = Field(default=0, ge=0, le=100000)
    dropped_events: int = Field(default=0, ge=0)
    last_error: str | None = Field(default=None, max_length=1000)

    @field_validator("cluster_id")
    @classmethod
    def valid_cluster_id(cls, value: str) -> str:
        if not CLUSTER_ID.fullmatch(value):
            raise ValueError("cluster_id contains unsupported characters")
        return value

    @field_validator("namespaces")
    @classmethod
    def valid_namespaces(cls, values: list[str]) -> list[str]:
        result = sorted(set(values))
        if any(not NAMESPACE.fullmatch(value) for value in result):
            raise ValueError("invalid Kubernetes namespace")
        return result


class HeartbeatInput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    cluster_id: str = Field(min_length=3, max_length=160)
    queue_depth: int = Field(default=0, ge=0, le=100000)
    dropped_events: int = Field(default=0, ge=0)
    last_error: str | None = Field(default=None, max_length=1000)


class AgentEventInput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    event_id: str = Field(min_length=8, max_length=160)
    type: str = Field(min_length=1, max_length=128)
    at_unix_ms: int = Field(default=0, ge=0)
    namespace: str = Field(default="", max_length=253)
    name: str = Field(default="", max_length=253)
    kind: str = Field(default="", max_length=64)
    reason: str = Field(default="", max_length=256)
    message: str = Field(default="", max_length=2000)
    labels: dict[str, str] = Field(default_factory=dict)
    fields: dict[str, str] = Field(default_factory=dict)

    @field_validator("namespace")
    @classmethod
    def valid_namespace(cls, value: str) -> str:
        if value and not NAMESPACE.fullmatch(value):
            raise ValueError("invalid Kubernetes namespace")
        return value

    @field_validator("labels", "fields")
    @classmethod
    def bounded_mapping(cls, value: dict[str, str]) -> dict[str, str]:
        if len(value) > 64:
            raise ValueError("too many metadata fields")
        result = {}
        for key, item in value.items():
            if len(key) > 253 or len(item) > 1000:
                raise ValueError("metadata field is too large")
            result[key] = item
        return result


class EventBatchInput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    cluster_id: str = Field(min_length=3, max_length=160)
    events: list[AgentEventInput] = Field(min_length=1, max_length=200)


class ScopeInput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    namespaces: list[str] = Field(default_factory=list, max_length=2000)

    @field_validator("namespaces")
    @classmethod
    def valid_namespaces(cls, values: list[str]) -> list[str]:
        result = sorted(set(values))
        if any(not NAMESPACE.fullmatch(value) for value in result):
            raise ValueError("invalid Kubernetes namespace")
        return result


def router(
    agent_token: str,
    admin_token: str,
    db_path: str,
    signals: SignalStore,
    database_url: str = "",
) -> APIRouter:
    enabled = bool(agent_token and admin_token and (db_path or database_url))
    store = create_kubernetes_store(db_path, database_url) if enabled else None
    stop = threading.Event()

    def agent_authorize(authorization: str | None) -> None:
        expected = "Bearer " + agent_token
        if not enabled:
            raise HTTPException(503, "Kubernetes module is not configured")
        if not authorization or not hmac.compare_digest(authorization, expected):
            raise HTTPException(401, "Kubernetes agent authentication required")

    def admin_authorize(authorization: str | None) -> None:
        expected = "Bearer " + admin_token
        if not enabled:
            raise HTTPException(503, "Kubernetes module is not configured")
        if not authorization or not hmac.compare_digest(authorization, expected):
            raise HTTPException(401, "Administrator authentication required")

    def config(cluster: dict) -> dict:
        return {
            "cluster_id": cluster["cluster_id"],
            "config_revision": cluster["config_revision"],
            "namespaces": cluster["selected_namespaces"],
            "poll_interval_s": 30,
            "max_batch_events": 100,
        }

    def feed(event: dict) -> None:
        at_unix_ms = int(event.get("at_unix_ms") or time.time() * 1000)
        # Ignore stale evidence when rebuilding the in-memory correlation window.
        if abs(int(time.time() * 1000) - at_unix_ms) > 3_600_000:
            return
        labels = {str(k): str(v) for k, v in (event.get("labels") or {}).items()}
        fields = {str(k): str(v) for k, v in (event.get("fields") or {}).items()}
        signals.add_k8s_event(
            K8sSignal(
                type=str(event.get("type") or "k8s.event"),
                at_unix_ms=at_unix_ms,
                cluster=str(event.get("cluster_id") or ""),
                namespace=str(event.get("namespace") or "") or None,
                name=str(event.get("name") or "") or None,
                kind=str(event.get("kind") or "") or None,
                reason=str(event.get("reason") or "") or None,
                message=str(event.get("message") or "") or None,
                labels=labels,
                fields=fields,
            )
        )
        if str(event.get("type") or "") in {"config.changed", "k8s.hpa.changed"}:
            entity = (
                labels.get("app")
                or labels.get("app.kubernetes.io/name")
                or str(event.get("name") or "")
            )
            signals.add_change_event(
                ChangeEvent(
                    kind="config.changed",
                    at_unix_ms=at_unix_ms,
                    entity=entity,
                    namespace=str(event.get("namespace") or "") or None,
                    metadata={
                        "cluster": event.get("cluster_id"),
                        "resource": event.get("kind"),
                        **fields,
                    },
                )
            )

    def bridge_loop() -> None:
        cursor = 0
        while not stop.is_set():
            rows = store.list_events(limit=1000, after_id=cursor, ascending=True)
            for row in rows:
                feed(row)
                cursor = max(cursor, int(row["id"]))
            stop.wait(1 if rows else 2)

    @asynccontextmanager
    async def lifespan(_app):
        if store is not None:
            store.initialize()
            stop.clear()
            thread = threading.Thread(target=bridge_loop, name="aidbg-kubernetes-bridge", daemon=True)
            thread.start()
        else:
            thread = None
        yield
        stop.set()
        if thread is not None:
            thread.join(timeout=5)

    routes = APIRouter(tags=["kubernetes"], lifespan=lifespan)

    @routes.post("/api/kubernetes/v1/register")
    def register(body: RegistrationInput, authorization: str | None = Header(default=None)):
        agent_authorize(authorization)
        saved = store.register(body.model_dump())
        return {"config": config(saved)}

    @routes.post("/api/kubernetes/v1/heartbeat")
    def heartbeat(body: HeartbeatInput, authorization: str | None = Header(default=None)):
        agent_authorize(authorization)
        cluster = store.heartbeat(
            body.cluster_id, queue_depth=body.queue_depth,
            dropped_events=body.dropped_events, last_error=body.last_error,
        )
        if cluster is None:
            raise HTTPException(404, "Cluster is not registered")
        return {"config": config(cluster)}

    @routes.get("/api/kubernetes/v1/config")
    def get_config(
        cluster_id: str = Query(min_length=3, max_length=160),
        authorization: str | None = Header(default=None),
    ):
        agent_authorize(authorization)
        cluster = store.get(cluster_id)
        if cluster is None:
            raise HTTPException(404, "Cluster is not registered")
        return {"config": config(cluster)}

    @routes.post("/api/kubernetes/v1/events")
    def events(body: EventBatchInput, authorization: str | None = Header(default=None)):
        agent_authorize(authorization)
        cluster = store.get(body.cluster_id)
        if cluster is None:
            raise HTTPException(404, "Cluster is not registered")
        allowed = set(cluster["selected_namespaces"])
        now_ms = int(time.time() * 1000)
        accepted_events = []
        rejected_scope = 0
        for item in body.events:
            event = item.model_dump()
            if event["namespace"] not in allowed:
                rejected_scope += 1
                continue
            if not event["at_unix_ms"]:
                event["at_unix_ms"] = now_ms
            if abs(now_ms - event["at_unix_ms"]) > 86_400_000:
                event["at_unix_ms"] = now_ms
            accepted_events.append(event)
        accepted, duplicates = store.add_events(body.cluster_id, accepted_events)
        return {
            "accepted": accepted,
            "duplicates": duplicates,
            "rejected_scope": rejected_scope,
            "config_revision": cluster["config_revision"],
        }

    @routes.get("/admin/kubernetes/status")
    def status(authorization: str | None = Header(default=None)):
        admin_authorize(authorization)
        clusters = store.list()
        return {
            "enabled": True,
            "clusters": len(clusters),
            "online": sum(1 for item in clusters if item["online"]),
            "monitoring": sum(1 for item in clusters if item["selected_namespaces"]),
            "events": store.count_events(),
        }

    @routes.get("/admin/kubernetes/clusters")
    def clusters(authorization: str | None = Header(default=None)):
        admin_authorize(authorization)
        return {"clusters": store.list()}

    @routes.get("/admin/kubernetes/clusters/{cluster_id}")
    def cluster(cluster_id: str, authorization: str | None = Header(default=None)):
        admin_authorize(authorization)
        saved = store.get(cluster_id)
        if saved is None:
            raise HTTPException(404, "Cluster is not registered")
        return saved

    @routes.put("/admin/kubernetes/clusters/{cluster_id}/scope")
    def update_scope(
        cluster_id: str,
        body: ScopeInput,
        authorization: str | None = Header(default=None),
    ):
        admin_authorize(authorization)
        saved = store.get(cluster_id)
        if saved is None:
            raise HTTPException(404, "Cluster is not registered")
        discovered = set(saved["namespaces"])
        unknown = sorted(set(body.namespaces) - discovered)
        if unknown:
            raise HTTPException(
                422,
                "Namespaces were not discovered by this cluster: " + ", ".join(unknown),
            )
        return store.update_scope(cluster_id, body.namespaces)

    @routes.delete("/admin/kubernetes/clusters/{cluster_id}")
    def delete_cluster(cluster_id: str, authorization: str | None = Header(default=None)):
        admin_authorize(authorization)
        if not store.delete(cluster_id):
            raise HTTPException(404, "Cluster is not registered")
        return {"deleted": True}

    @routes.get("/admin/kubernetes/events")
    def recent_events(
        cluster_id: str | None = None,
        limit: int = Query(default=100, ge=1, le=1000),
        authorization: str | None = Header(default=None),
    ):
        admin_authorize(authorization)
        return {"events": store.list_events(cluster_id, limit=limit)}

    return routes
