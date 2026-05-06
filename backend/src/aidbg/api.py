from __future__ import annotations

import time
import base64
import json
from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import RedirectResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from .config import Settings
from .engine import K8sSignal
from .explain_llm import explain_incident
from .ingest_otlp import StatsStore


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


def create_app(stats: StatsStore, settings: Settings) -> FastAPI:
    app = FastAPI(title="aidbg-backend", version="0.1.0")

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
    def ingest_stats() -> dict:
        s = stats.snapshot()
        return {
            "requests_received": s.requests_received,
            "spans_received": s.spans_received,
            "last_ingest_unix_s": s.last_ingest_unix_s,
        }

    @app.post("/ingest/k8s")
    def ingest_k8s(payload: K8sEventIn) -> dict:
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

    @app.get("/stats/services")
    def service_stats(window_s: int = 300) -> dict:
        return {"window_s": window_s, "services": stats.signals.service_metrics(window_s)}

    @app.get("/graph/snapshot")
    def graph_snapshot(window_s: int = 300) -> dict:
        return stats.signals.graph_snapshot(window_s)

    @app.get("/anomalies/latest")
    def anomalies_latest(
        current_window_s: int | None = None,
        baseline_window_s: int | None = None,
    ) -> dict:
        cur = current_window_s or settings.current_window_s
        base = baseline_window_s or settings.baseline_window_s
        return {"anomalies": stats.signals.detect_anomalies(cur, base)}

    @app.get("/incident/latest")
    def incident_latest(
        current_window_s: int | None = None,
        baseline_window_s: int | None = None,
    ) -> dict:
        cur = current_window_s or settings.current_window_s
        base = baseline_window_s or settings.baseline_window_s
        return stats.signals.build_incident(cur, base)

    @app.get("/explain/latest")
    def explain_latest(
        current_window_s: int | None = None,
        baseline_window_s: int | None = None,
    ) -> dict:
        cur = current_window_s or settings.current_window_s
        base = baseline_window_s or settings.baseline_window_s
        incident = stats.signals.build_incident(cur, base)
        return explain_incident(incident, settings)

    @app.get("/k8s/events")
    def k8s_events(limit: int = 100) -> dict:
        return {"events": stats.signals.recent_k8s_events(limit)}

    @app.get("/query/why_slow")
    def query_why_slow(
        service: str,
        current_window_s: int | None = None,
        baseline_window_s: int | None = None,
    ) -> dict:
        cur = current_window_s or settings.current_window_s
        base = baseline_window_s or settings.baseline_window_s
        return stats.signals.why_service_slow(service, cur, base)

    @app.get("/query/evidence")
    def query_evidence(
        service: str,
        current_window_s: int | None = None,
        baseline_window_s: int | None = None,
    ) -> dict:
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
                    {"op": "add", "path": "/spec/containers/0/env/-", "value": {"name": "OTEL_SERVICE_NAME", "value": metadata.get("name", "unknown")}},
                    {"op": "add", "path": "/spec/containers/0/env/-", "value": {"name": "OTEL_EXPORTER_OTLP_ENDPOINT", "value": "http://aidbg-otel-collector.aidbg-otel:4318"}},
                    {"op": "add", "path": "/spec/containers/0/env/-", "value": {"name": "OTEL_TRACES_EXPORTER", "value": "otlp"}},
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

