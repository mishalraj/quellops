from __future__ import annotations

import os
import threading

import uvicorn

from .api import create_app
from .config import load_settings
from .ingest_otlp import StatsStore, serve_otlp_grpc


def _split_host_port(bind: str) -> tuple[str, int]:
    host, port_s = bind.rsplit(":", 1)
    return host, int(port_s)


def main() -> None:
    settings = load_settings()
    stats = StatsStore()

    # Start OTLP gRPC receiver
    otlp_server = serve_otlp_grpc(settings.otlp_grpc_bind, stats)

    # Start HTTP API (FastAPI)
    app = create_app(stats)
    http_host, http_port = _split_host_port(settings.http_bind)

    config = uvicorn.Config(app=app, host=http_host, port=http_port, log_level="info")
    server = uvicorn.Server(config)

    try:
        server.run()
    finally:
        otlp_server.stop(grace=None)


if __name__ == "__main__":
    main()

