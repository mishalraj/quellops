from __future__ import annotations

import hmac
import hashlib
import re
import secrets
import threading
import time
from contextlib import asynccontextmanager

from fastapi import APIRouter, Header, HTTPException, Query
from pydantic import BaseModel, ConfigDict, Field, field_validator

from .engine import ChangeEvent, K8sSignal, SignalStore
from .incident_store import create_incident_store, is_meaningful_incident
from .kafka_transport import KafkaEventConsumer, KafkaOutboxPublisher
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


class TokenRotationInput(BaseModel):
    grace_s: int = Field(default=600, ge=0, le=86400)


def feed_kubernetes_event(signals: SignalStore, event: dict) -> None:
    at_unix_ms = int(event.get("at_unix_ms") or time.time() * 1000)
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


def process_kubernetes_event(
    signals: SignalStore,
    incident_store,
    event: dict,
    incident_builder=None,
) -> bool:
    feed_kubernetes_event(signals, event)
    builder = incident_builder or (lambda: signals.build_incident(300, 900))
    incident = builder()
    if not is_meaningful_incident(incident):
        return False
    cluster_id = str(event.get("cluster_id") or "")
    event_id = str(event.get("event_id") or "")
    incident_store.save(
        f"event:{cluster_id}:{event_id}",
        incident,
        trigger_type="kubernetes_event",
        trigger_id=event_id,
    )
    return True


def router(
    agent_token: str,
    admin_token: str,
    db_path: str,
    signals: SignalStore,
    database_url: str = "",
    event_transport: str = "database",
    kafka_bootstrap_servers: str = "",
    kafka_topic: str = "quellops.kubernetes.events.v1",
    kafka_config: dict[str, str] | None = None,
    retention_events: int = 5000,
    kafka_consumer_group: str = "quellops-central-v1",
    run_kafka_consumer: bool = True,
    incident_store=None,
    incident_builder=None,
) -> APIRouter:
    event_transport = event_transport.strip().lower()
    if event_transport not in {"database", "kafka"}:
        raise ValueError("AIDBG_EVENT_TRANSPORT must be database or kafka")
    enabled = bool(agent_token and admin_token and (db_path or database_url))
    store = (
        create_kubernetes_store(db_path, database_url, retention_events)
        if enabled
        else None
    )
    incident_store = incident_store or (
        create_incident_store(db_path, database_url) if enabled else None
    )
    publisher = (
        KafkaOutboxPublisher(
            store, kafka_bootstrap_servers, kafka_topic, producer_config=kafka_config
        )
        if enabled and event_transport == "kafka"
        else None
    )
    consumer = (
        KafkaEventConsumer(
            kafka_bootstrap_servers,
            kafka_topic,
            kafka_consumer_group,
            lambda event: process_event(event),
            consumer_config=kafka_config,
        )
        if enabled and event_transport == "kafka" and run_kafka_consumer
        else None
    )
    stop = threading.Event()

    def bearer_token(authorization: str | None) -> str:
        if not enabled:
            raise HTTPException(503, "Kubernetes module is not configured")
        if not authorization or not authorization.startswith("Bearer "):
            raise HTTPException(401, "Kubernetes agent authentication required")
        return authorization[7:]

    def token_hash(token: str) -> str:
        return hmac.new(
            agent_token.encode("utf-8"), token.encode("utf-8"), hashlib.sha256
        ).hexdigest()

    def bootstrap_authorize(authorization: str | None) -> None:
        if not hmac.compare_digest(bearer_token(authorization), agent_token):
            raise HTTPException(401, "Kubernetes bootstrap authentication required")

    def cluster_authorize(cluster_id: str, authorization: str | None) -> None:
        supplied = token_hash(bearer_token(authorization))
        credentials = store.agent_credentials(cluster_id)
        if credentials is None:
            raise HTTPException(404, "Cluster is not registered")
        current = str(credentials.get("agent_token_hash") or "")
        previous = str(credentials.get("previous_agent_token_hash") or "")
        previous_until = float(credentials.get("previous_agent_token_valid_until") or 0)
        valid = bool(current and hmac.compare_digest(supplied, current))
        if previous and previous_until >= time.time():
            valid = valid or hmac.compare_digest(supplied, previous)
        if not valid:
            raise HTTPException(401, "Cluster credential is invalid or expired")

    def admin_authorize(authorization: str | None) -> None:
        expected = "Bearer " + admin_token
        if not enabled:
            raise HTTPException(503, "Kubernetes module is not configured")
        if not authorization or not hmac.compare_digest(authorization, expected):
            raise HTTPException(401, "Administrator authentication required")

    def config(cluster: dict, issued_token: str = "") -> dict:
        result = {
            "cluster_id": cluster["cluster_id"],
            "config_revision": cluster["config_revision"],
            "namespaces": cluster["selected_namespaces"],
            "poll_interval_s": 30,
            "max_batch_events": 100,
        }
        if issued_token:
            result["agent_token"] = issued_token
        return result

    def feed(event: dict) -> None:
        feed_kubernetes_event(signals, event)

    def process_event(event: dict) -> None:
        process_kubernetes_event(
            signals,
            incident_store,
            event,
            incident_builder=incident_builder,
        )

    def bridge_loop() -> None:
        cursor = 0
        while not stop.is_set():
            rows = store.list_events(limit=1000, after_id=cursor, ascending=True)
            for row in rows:
                process_event(row)
                cursor = max(cursor, int(row["id"]))
            stop.wait(1 if rows else 2)

    @asynccontextmanager
    async def lifespan(_app):
        if store is not None:
            store.initialize()
            incident_store.initialize()
            stop.clear()
            if event_transport == "database":
                thread = threading.Thread(target=bridge_loop, name="quellops-kubernetes-bridge", daemon=True)
                thread.start()
            else:
                thread = None
            if publisher is not None:
                publisher.start()
            if consumer is not None:
                consumer.start()
        else:
            thread = None
        yield
        stop.set()
        if publisher is not None:
            publisher.stop()
        if consumer is not None:
            consumer.stop()
        if thread is not None:
            thread.join(timeout=5)

    routes = APIRouter(tags=["kubernetes"], lifespan=lifespan)

    @routes.post("/api/kubernetes/v1/register")
    def register(body: RegistrationInput, authorization: str | None = Header(default=None)):
        credentials = store.agent_credentials(body.cluster_id)
        if credentials and credentials.get("agent_token_hash"):
            cluster_authorize(body.cluster_id, authorization)
        else:
            bootstrap_authorize(authorization)
        saved = store.register(body.model_dump())
        issued_token = ""
        if not credentials or not credentials.get("agent_token_hash"):
            issued_token = secrets.token_urlsafe(32)
            store.rotate_agent_token(body.cluster_id, token_hash(issued_token), grace_s=0)
            saved = store.get(body.cluster_id)
        return {"config": config(saved, issued_token)}

    @routes.post("/api/kubernetes/v1/heartbeat")
    def heartbeat(body: HeartbeatInput, authorization: str | None = Header(default=None)):
        cluster_authorize(body.cluster_id, authorization)
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
        cluster_authorize(cluster_id, authorization)
        cluster = store.get(cluster_id)
        if cluster is None:
            raise HTTPException(404, "Cluster is not registered")
        return {"config": config(cluster)}

    @routes.post("/api/kubernetes/v1/events")
    def events(body: EventBatchInput, authorization: str | None = Header(default=None)):
        cluster_authorize(body.cluster_id, authorization)
        cluster = store.get(body.cluster_id)
        if cluster is None:
            raise HTTPException(404, "Cluster is not registered")
        allowed = set(cluster["selected_namespaces"])
        now_ms = int(time.time() * 1000)
        accepted_events = []
        rejected_scope = 0
        for item in body.events:
            event = item.model_dump()
            if (event["namespace"] and event["namespace"] not in allowed) or (
                not event["namespace"] and not allowed
            ):
                rejected_scope += 1
                continue
            if not event["at_unix_ms"]:
                event["at_unix_ms"] = now_ms
            if abs(now_ms - event["at_unix_ms"]) > 86_400_000:
                event["at_unix_ms"] = now_ms
            accepted_events.append(event)
        accepted, duplicates = store.add_events(body.cluster_id, accepted_events)
        if event_transport == "database":
            store.mark_events_local(
                body.cluster_id, [event["event_id"] for event in accepted_events]
            )
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
        transport = (
            publisher.status()
            if publisher is not None
            else {
                "mode": "database",
                "healthy": True,
                "topic": None,
                "backlog": 0,
                "last_error": None,
                "last_publish_at": None,
            }
        )
        if consumer is not None:
            consumer_status = consumer.status()
            transport["consumer"] = consumer_status
            transport["healthy"] = bool(
                transport.get("healthy") and consumer_status.get("healthy")
            )
        elif event_transport == "kafka":
            consumer_status = incident_store.worker_status(kafka_consumer_group) or {
                "group": kafka_consumer_group,
                "healthy": False,
                "last_error": "consumer worker has not reported status",
            }
            transport["consumer"] = consumer_status
            transport["healthy"] = bool(
                transport.get("healthy") and consumer_status.get("healthy")
            )
        return {
            "enabled": True,
            "clusters": len(clusters),
            "online": sum(1 for item in clusters if item["online"]),
            "monitoring": sum(1 for item in clusters if item["selected_namespaces"]),
            "events": store.count_events(),
            "retention_events_per_cluster": store.retention_events,
            "transport": transport,
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

    @routes.post("/admin/kubernetes/clusters/{cluster_id}/rotate-token")
    def rotate_token(
        cluster_id: str,
        body: TokenRotationInput,
        authorization: str | None = Header(default=None),
    ):
        admin_authorize(authorization)
        if store.get(cluster_id) is None:
            raise HTTPException(404, "Cluster is not registered")
        raw_token = secrets.token_urlsafe(32)
        store.rotate_agent_token(cluster_id, token_hash(raw_token), grace_s=body.grace_s)
        return {
            "cluster_id": cluster_id,
            "agent_token": raw_token,
            "previous_token_valid_for_s": body.grace_s,
        }

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
