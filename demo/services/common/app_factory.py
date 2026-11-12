from __future__ import annotations

import httpx
from fastapi import FastAPI, HTTPException

from .faults import apply_faults, downstream_url
from .otel_setup import instrument_fastapi, setup_otel


def create_proxy_app(service_name: str, path: str = "/") -> FastAPI:
    setup_otel(service_name)
    app = FastAPI(title=service_name)

    @app.get("/healthz")
    async def healthz() -> dict:
        return {"ok": True, "service": service_name}

    @app.get(path)
    async def handle() -> dict:
        if not await apply_faults():
            raise HTTPException(status_code=500, detail="fault_injected")
        downstream = downstream_url()
        if downstream:
            async with httpx.AsyncClient(timeout=30.0) as client:
                r = await client.get(downstream.rstrip("/") + "/")
                r.raise_for_status()
                body = r.json()
            return {"service": service_name, "downstream": body}
        return {"service": service_name, "items": [{"id": 1, "name": "widget"}]}

    instrument_fastapi(app)
    return app


def create_leaf_app(service_name: str) -> FastAPI:
    return create_proxy_app(service_name, path="/")
