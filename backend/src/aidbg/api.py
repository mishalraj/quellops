from __future__ import annotations

from fastapi import FastAPI

from .ingest_otlp import StatsStore


def create_app(stats: StatsStore) -> FastAPI:
    app = FastAPI(title="aidbg-backend", version="0.1.0")

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

    return app

