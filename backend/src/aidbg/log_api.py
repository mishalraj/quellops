"""HTTP boundary for the UI; all log work is delegated to the Go gRPC service."""
from __future__ import annotations

import hmac
from contextlib import asynccontextmanager

from fastapi import APIRouter, Header, HTTPException
from pydantic import BaseModel, Field

from .log_client import LogClient, LogClientError


class FetchRequest(BaseModel):
    source: str
    start: float
    end: float
    limit: int = Field(default=100, ge=1, le=10000)


class InvestigateRequest(BaseModel):
    source: str
    start: float
    end: float
    target_records: int | None = Field(default=None, ge=1, le=50000)


def router(target: str, service_token: str, api_token: str) -> APIRouter:
    client = LogClient(target, service_token) if target and service_token else None

    @asynccontextmanager
    async def lifespan(_app):
        yield
        if client is not None:
            client.close()

    routes = APIRouter(prefix="/logs", tags=["logs"], lifespan=lifespan)

    def authorize(authorization: str | None) -> None:
        if client is None or not api_token:
            raise HTTPException(503, "Log API disabled; configure the Go logs service")
        if not authorization or not hmac.compare_digest(
            authorization, "Bearer " + api_token
        ):
            raise HTTPException(401, "Log API authentication required")

    def call(operation):
        try:
            return operation()
        except LogClientError as exc:
            raise HTTPException(503 if exc.unavailable else 400, str(exc)) from None

    @routes.get("/sources")
    def sources(authorization: str | None = Header(default=None)):
        authorize(authorization)
        return call(client.list_sources)

    @routes.post("/fetch")
    def fetch(body: FetchRequest, authorization: str | None = Header(default=None)):
        authorize(authorization)
        return call(lambda: client.fetch(body.source, body.start, body.end, body.limit))

    @routes.post("/investigate")
    def investigate(
        body: InvestigateRequest,
        authorization: str | None = Header(default=None),
    ):
        authorize(authorization)
        return call(
            lambda: client.investigate(
                body.source, body.start, body.end, body.target_records
            )
        )

    return routes
