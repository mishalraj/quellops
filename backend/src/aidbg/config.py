from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path


@dataclass(frozen=True)
class Settings:
    # OTLP gRPC receiver bind (collector exports here)
    otlp_grpc_bind: str = os.getenv("AIDBG_OTLP_GRPC_BIND", "0.0.0.0:4317")
    # Minimal HTTP API bind (health + basic stats)
    http_bind: str = os.getenv("AIDBG_HTTP_BIND", "0.0.0.0:8088")
    # Analysis windows in seconds (short windows for local MVP).
    current_window_s: int = int(os.getenv("AIDBG_CURRENT_WINDOW_S", "300"))
    baseline_window_s: int = int(os.getenv("AIDBG_BASELINE_WINDOW_S", "900"))
    # Optional Azure OpenAI config for explanation layer.
    azure_openai_endpoint: str = os.getenv("AZURE_OPENAI_ENDPOINT", "")
    azure_openai_api_key: str = os.getenv("AZURE_OPENAI_API_KEY", "")
    azure_openai_api_version: str = os.getenv("AZURE_OPENAI_API_VERSION", "2024-12-01-preview")
    azure_openai_deployment: str = os.getenv("AZURE_OPENAI_DEPLOYMENT", "")
    service_map_path: str = os.getenv(
        "AIDBG_SERVICE_MAP_PATH",
        str(Path(__file__).resolve().parents[2] / "service_map.yaml"),
    )
    change_horizon_s: int = int(os.getenv("AIDBG_CHANGE_HORIZON_S", "900"))
    db_slow_query_threshold_ms: float = float(os.getenv("AIDBG_DB_SLOW_MS", "100"))


def load_settings() -> Settings:
    return Settings()

