from __future__ import annotations

import base64
import hashlib
import hmac
import json
import time
from pathlib import Path

from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.responses import RedirectResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from .change_ingest import github_push_to_change_events, load_service_map
from .config import Settings
from .database_control import router as database_control_router
from .engine import ChangeEvent, K8sSignal
from .explain_llm import explain_incident
from .incident_store import create_incident_store, is_meaningful_incident
from .ingest_otlp import StatsStore
from .k8s_control import router as k8s_control_router
from .log_api import router as log_router
from .log_control import router as log_control_router


class K8sEventIn(BaseModel):
    type: str
    at_unix_ms: int = Field(default_factory=lambda: 0)
    cluster: str | None = None
    namespace: str | None = None
    name: str | None = None
    kind: str | None = None
    reason: str | None = None
    message: str | None = None
    labels: dict[str, str] = Field(default_factory=dict)
    fields: dict[str, str] = Field(default_factory=dict)


class ChangeEventIn(BaseModel):
    kind: str
    at_unix_ms: int = Field(default_factory=lambda: 0)
    entity: str
    namespace: str | None = None
    metadata: dict = Field(default_factory=dict)


def create_app(stats: StatsStore, settings: Settings) -> FastAPI:
    app = FastAPI(title="QuellOps", version="0.1.0")
    incident_store = create_incident_store(
        settings.control_db_path,
        settings.control_database_url,
        settings.incident_retention,
    )
    app.include_router(
        log_router(
            settings.log_service_grpc_target,
            settings.log_service_token,
            settings.log_api_token,
            settings.log_tls_ca,
            settings.log_tls_server_name,
        )
    )
    app.include_router(
        log_control_router(
            settings.log_service_grpc_target,
            settings.log_service_token,
            settings.log_admin_token,
            settings.log_master_key,
            settings.control_db_path,
            stats.signals,
            settings.control_database_url,
            settings.log_tls_ca,
            settings.log_tls_server_name,
        )
    )
    app.include_router(
        k8s_control_router(
            agent_token=settings.k8s_agent_token,
            admin_token=settings.log_admin_token,
            db_path=settings.control_db_path,
            signals=stats.signals,
            database_url=settings.control_database_url,
            event_transport=settings.event_transport,
            kafka_bootstrap_servers=settings.kafka_bootstrap_servers,
            kafka_topic=settings.kafka_k8s_topic,
            kafka_config={
                key: value
                for key, value in {
                    "security.protocol": settings.kafka_security_protocol,
                    "sasl.mechanism": settings.kafka_sasl_mechanism,
                    "sasl.username": settings.kafka_sasl_username,
                    "sasl.password": settings.kafka_sasl_password,
                    "ssl.ca.location": settings.kafka_ssl_ca_location,
                }.items()
                if value
            },
            retention_events=settings.k8s_retention_events,
            kafka_consumer_group=settings.kafka_consumer_group,
            run_kafka_consumer=settings.kafka_consumer_embedded,
            incident_store=incident_store,
            incident_builder=lambda: stats.signals.build_incident(
                settings.current_window_s, settings.baseline_window_s
            ),
        )
    )
    app.include_router(
        database_control_router(
            admin_token=settings.log_admin_token,
            master_key=settings.log_master_key,
            db_path=settings.control_db_path,
            signals=stats.signals,
            retention=settings.database_signal_retention,
            database_url=settings.control_database_url,
        )
    )
    service_map = load_service_map(Path(settings.service_map_path))
    incident_store_ready = False

    def durable_incident(cur: int, base: int) -> dict:
        nonlocal incident_store_ready
        if not incident_store_ready:
            incident_store.initialize()
            incident_store_ready = True
        current = stats.signals.build_incident(cur, base)
        if is_meaningful_incident(current):
            generated = int(current.get("generated_at_unix_ms") or time.time() * 1000)
            return incident_store.save(
                f"snapshot:{generated}",
                current,
                trigger_type="api_snapshot",
                trigger_id=str(generated),
            )
        return incident_store.latest() or current

    def authorize_admin(authorization: str | None) -> None:
        expected = "Bearer " + settings.log_admin_token
        if not settings.log_admin_token:
            raise HTTPException(503, "Administrator API is not configured")
        if not authorization or not hmac.compare_digest(authorization, expected):
            raise HTTPException(401, "Administrator authentication required")

    def authorize_change_ingest(authorization: str | None) -> None:
        if not settings.change_ingest_token:
            raise HTTPException(503, "Change ingestion is not configured")
        expected = "Bearer " + settings.change_ingest_token
        if not authorization or not hmac.compare_digest(authorization, expected):
            raise HTTPException(401, "Change ingestion authentication required")

    def _store_change(payload: ChangeEventIn) -> None:
        at_unix_ms = payload.at_unix_ms or int(time.time() * 1000)
        stats.signals.add_change_event(
            ChangeEvent(
                kind=payload.kind,
                at_unix_ms=at_unix_ms,
                entity=payload.entity,
                namespace=payload.namespace,
                metadata=payload.metadata,
            )
        )

    static_dir = Path(__file__).resolve().parent / "static"
    if static_dir.is_dir():
        app.mount("/ui", StaticFiles(directory=str(static_dir), html=True), name="ui")

    @app.get("/")
    def root_redirect() -> RedirectResponse:
        if static_dir.is_dir():
            return RedirectResponse(url="/ui/", status_code=307)
        return RedirectResponse(url="/docs", status_code=307)

    @app.get("/healthz")
    def healthz() -> dict:
        return {"ok": True}

    @app.get("/stats/ingest")
    def ingest_stats(authorization: str | None = Header(default=None)) -> dict:
        authorize_admin(authorization)
        s = stats.snapshot()
        return {
            "requests_received": s.requests_received,
            "spans_received": s.spans_received,
            "last_ingest_unix_s": s.last_ingest_unix_s,
        }

    @app.post("/ingest/k8s")
    def ingest_k8s(payload: K8sEventIn, authorization: str | None = Header(default=None)) -> dict:
        # Compatibility endpoint for older agents. Production agents use the
        # durable batched API, but this path must not permit signal injection.
        if not settings.k8s_agent_token:
            raise HTTPException(503, "Kubernetes agent ingestion is not configured")
        expected = "Bearer " + settings.k8s_agent_token
        if not authorization or not hmac.compare_digest(authorization, expected):
            raise HTTPException(401, "Kubernetes agent authentication required")
        at_unix_ms = payload.at_unix_ms or int(time.time() * 1000)
        stats.signals.add_k8s_event(
            K8sSignal(
                type=payload.type,
                at_unix_ms=at_unix_ms,
                cluster=payload.cluster,
                namespace=payload.namespace,
                name=payload.name,
                kind=payload.kind,
                reason=payload.reason,
                message=payload.message,
                labels=payload.labels,
                fields=payload.fields,
            )
        )
        return {"ok": True}

    @app.post("/ingest/change")
    def ingest_change(
        payload: ChangeEventIn,
        authorization: str | None = Header(default=None),
    ) -> dict:
        authorize_change_ingest(authorization)
        _store_change(payload)
        return {"ok": True}

    @app.get("/changes/recent")
    def changes_recent(
        limit: int = 100,
        authorization: str | None = Header(default=None),
    ) -> dict:
        authorize_admin(authorization)
        return {"changes": stats.signals.recent_change_events(limit)}

    @app.post("/webhook/github")
    async def github_webhook(request: Request) -> dict:
        if not settings.github_webhook_secret:
            raise HTTPException(503, "GitHub webhook is not configured")
        content_length = request.headers.get("content-length")
        if content_length:
            try:
                if int(content_length) > 1_048_576:
                    raise HTTPException(413, "GitHub webhook payload is too large")
            except ValueError as exc:
                raise HTTPException(400, "Invalid Content-Length header") from exc
        raw_payload = await request.body()
        if len(raw_payload) > 1_048_576:
            raise HTTPException(413, "GitHub webhook payload is too large")
        supplied_signature = request.headers.get("x-hub-signature-256", "")
        expected_signature = (
            "sha256="
            + hmac.new(
                settings.github_webhook_secret.encode("utf-8"),
                raw_payload,
                hashlib.sha256,
            ).hexdigest()
        )
        if not supplied_signature or not hmac.compare_digest(
            supplied_signature, expected_signature
        ):
            raise HTTPException(401, "Invalid GitHub webhook signature")
        try:
            payload = json.loads(raw_payload)
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            raise HTTPException(400, "Invalid GitHub webhook JSON") from exc
        if not isinstance(payload, dict):
            raise HTTPException(400, "GitHub webhook payload must be an object")
        if payload.get("zen") is not None:
            return {"ok": True, "ping": True}
        if payload.get("deleted"):
            return {"ok": True, "ignored": "deleted_branch"}
        events = github_push_to_change_events(payload, service_map)
        for ev in events:
            _store_change(ChangeEventIn(**ev))
        return {"ok": True, "events": len(events)}

    @app.get("/stats/services")
    def service_stats(
        window_s: int = 300,
        authorization: str | None = Header(default=None),
    ) -> dict:
        authorize_admin(authorization)
        return {"window_s": window_s, "services": stats.signals.service_metrics(window_s)}

    @app.get("/graph/snapshot")
    def graph_snapshot(
        window_s: int = 300,
        authorization: str | None = Header(default=None),
    ) -> dict:
        authorize_admin(authorization)
        return stats.signals.graph_snapshot(window_s)

    @app.get("/trace/focus")
    def trace_focus(
        service: str,
        window_s: int = 300,
        authorization: str | None = Header(default=None),
    ) -> dict:
        """One representative trace touching `service` (ancestors + subtree), for Gantt UI."""
        authorize_admin(authorization)
        snap = stats.signals.trace_focus_snapshot(service, window_s)
        return {"window_s": window_s, "service": service, "trace": snap}

    @app.get("/anomalies/latest")
    def anomalies_latest(
        current_window_s: int | None = None,
        baseline_window_s: int | None = None,
        authorization: str | None = Header(default=None),
    ) -> dict:
        authorize_admin(authorization)
        cur = current_window_s or settings.current_window_s
        base = baseline_window_s or settings.baseline_window_s
        return {"anomalies": stats.signals.detect_anomalies(cur, base)}

    @app.get("/incident/latest")
    def incident_latest(
        current_window_s: int | None = None,
        baseline_window_s: int | None = None,
        authorization: str | None = Header(default=None),
    ) -> dict:
        authorize_admin(authorization)
        cur = current_window_s or settings.current_window_s
        base = baseline_window_s or settings.baseline_window_s
        return durable_incident(cur, base)

    @app.get("/admin/incidents/history")
    def incident_history(
        limit: int = 100,
        authorization: str | None = Header(default=None),
    ) -> dict:
        nonlocal incident_store_ready
        authorize_admin(authorization)
        if not incident_store_ready:
            incident_store.initialize()
            incident_store_ready = True
        return {"incidents": incident_store.list(limit)}

    @app.get("/explain/latest")
    def explain_latest(
        current_window_s: int | None = None,
        baseline_window_s: int | None = None,
        authorization: str | None = Header(default=None),
    ) -> dict:
        authorize_admin(authorization)
        cur = current_window_s or settings.current_window_s
        base = baseline_window_s or settings.baseline_window_s
        incident = durable_incident(cur, base)
        return explain_incident(incident, settings)

    @app.get("/k8s/events")
    def k8s_events(
        limit: int = 100,
        authorization: str | None = Header(default=None),
    ) -> dict:
        authorize_admin(authorization)
        return {"events": stats.signals.recent_k8s_events(limit)}

    @app.get("/query/why_slow")
    def query_why_slow(
        service: str,
        current_window_s: int | None = None,
        baseline_window_s: int | None = None,
        authorization: str | None = Header(default=None),
    ) -> dict:
        authorize_admin(authorization)
        cur = current_window_s or settings.current_window_s
        base = baseline_window_s or settings.baseline_window_s
        return stats.signals.why_service_slow(service, cur, base)

    @app.get("/query/evidence")
    def query_evidence(
        service: str,
        current_window_s: int | None = None,
        baseline_window_s: int | None = None,
        authorization: str | None = Header(default=None),
    ) -> dict:
        authorize_admin(authorization)
        cur = current_window_s or settings.current_window_s
        base = baseline_window_s or settings.baseline_window_s
        return stats.signals.evidence_bundle(service, cur, base)

    @app.post("/webhook/mutate")
    def mutate_webhook(payload: dict) -> dict:
        req = payload.get("request", {})
        uid = req.get("uid", "")
        obj = req.get("object", {}) or {}
        metadata = obj.get("metadata", {}) or {}
        labels = metadata.get("labels", {}) or {}
        enabled = labels.get("aidbg-instrument", "false").lower() == "true"
        patch_ops = []
        if enabled:
            containers = (obj.get("spec", {}) or {}).get("containers", []) or []
            first = containers[0] if containers else {}
            if "env" not in first:
                patch_ops.append({"op": "add", "path": "/spec/containers/0/env", "value": []})
            patch_ops.extend(
                [
                    {
                        "op": "add",
                        "path": "/spec/containers/0/env/-",
                        "value": {
                            "name": "OTEL_SERVICE_NAME",
                            "value": metadata.get("name", "unknown"),
                        },
                    },
                    {
                        "op": "add",
                        "path": "/spec/containers/0/env/-",
                        "value": {
                            "name": "OTEL_EXPORTER_OTLP_ENDPOINT",
                            "value": "http://quellops-otel-collector.quellops-otel:4318",
                        },
                    },
                    {
                        "op": "add",
                        "path": "/spec/containers/0/env/-",
                        "value": {"name": "OTEL_TRACES_EXPORTER", "value": "otlp"},
                    },
                ]
            )
        patch_b64 = base64.b64encode(json.dumps(patch_ops).encode("utf-8")).decode("utf-8")
        return {
            "apiVersion": "admission.k8s.io/v1",
            "kind": "AdmissionReview",
            "response": {
                "uid": uid,
                "allowed": True,
                "patchType": "JSONPatch",
                "patch": patch_b64,
            },
        }

    return app
