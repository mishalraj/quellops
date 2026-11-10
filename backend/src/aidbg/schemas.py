from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass
class SpanSummary:
    service: str
    request_count: int
    error_rate: float
    p50_latency_ms: float
    p95_latency_ms: float


@dataclass
class GraphEdge:
    source: str
    target: str
    request_count: int
    error_rate: float
    p95_latency_ms: float


@dataclass
class Anomaly:
    kind: str
    entity: str
    magnitude: float
    start_unix_s: float
    details: dict[str, Any]


@dataclass
class CandidateCause:
    type: str
    entity: str
    score: float
    evidence: dict[str, Any]


@dataclass
class Incident:
    generated_at_unix_ms: int
    anomalies: list[dict[str, Any]]
    top_causes: list[dict[str, Any]]
    graph_snapshot: dict[str, Any]
    log_evidence: list[dict[str, Any]]
