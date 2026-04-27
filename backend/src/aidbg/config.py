from __future__ import annotations

from dataclasses import dataclass
import os


@dataclass(frozen=True)
class Settings:
    # OTLP gRPC receiver bind (collector exports here)
    otlp_grpc_bind: str = os.getenv("AIDBG_OTLP_GRPC_BIND", "0.0.0.0:4317")
    # Minimal HTTP API bind (health + basic stats)
    http_bind: str = os.getenv("AIDBG_HTTP_BIND", "0.0.0.0:8088")


def load_settings() -> Settings:
    return Settings()

