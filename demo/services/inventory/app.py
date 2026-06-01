from __future__ import annotations

import os

import httpx
import uvicorn
from fastapi import FastAPI, HTTPException

from services.common.faults import apply_faults, downstream_url
from services.common.migrate import ensure_schema
from services.common.otel_setup import instrument_fastapi, setup_otel
from services.common.db_setup import setup_db_otel
from services.inventory import db

SERVICE_NAME = os.getenv("SERVICE_NAME", "inventory")


def create_app() -> FastAPI:
    setup_otel(SERVICE_NAME)
    setup_db_otel()
    if os.getenv("SKIP_DB_MIGRATE", "").lower() not in ("1", "true", "yes"):
        ensure_schema()

    app = FastAPI(title=SERVICE_NAME)

    @app.get("/healthz")
    async def healthz() -> dict:
        return {"ok": True, "service": SERVICE_NAME}

    @app.get("/")
    async def handle() -> dict:
        if not await apply_faults():
            raise HTTPException(status_code=500, detail="fault_injected")
        item = db.fetch_item()
        items = db.list_items()
        body: dict = {"service": SERVICE_NAME, "item": item, "items": items}
        downstream = downstream_url()
        if downstream:
            async with httpx.AsyncClient(timeout=30.0) as client:
                r = await client.get(downstream.rstrip("/") + "/")
                r.raise_for_status()
                body["downstream"] = r.json()
        return body

    instrument_fastapi(app)
    return app


app = create_app()

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", "8080")))
