from __future__ import annotations

from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor
import threading
import time
from typing import Any

import grpc

from opentelemetry.proto.collector.trace.v1.trace_service_pb2_grpc import (
    TraceServiceServicer,
    add_TraceServiceServicer_to_server,
)
from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import (
    ExportTraceServiceResponse,
)

from .engine import SignalStore, SpanPoint


@dataclass
class IngestStats:
    spans_received: int = 0
    requests_received: int = 0
    last_ingest_unix_s: float | None = None


class StatsStore:
    def __init__(self, signals: SignalStore | None = None) -> None:
        self._lock = threading.Lock()
        self._stats = IngestStats()
        self._signals = signals or SignalStore()

    @property
    def signals(self) -> SignalStore:
        return self._signals

    def add(self, span_count: int, span_points: list[SpanPoint], edges: list[tuple[float, str, str, float, bool]]) -> None:
        now = time.time()
        with self._lock:
            self._stats.requests_received += 1
            self._stats.spans_received += span_count
            self._stats.last_ingest_unix_s = now
        self._signals.add_spans(span_points, edges)

    def snapshot(self) -> IngestStats:
        with self._lock:
            return IngestStats(
                spans_received=self._stats.spans_received,
                requests_received=self._stats.requests_received,
                last_ingest_unix_s=self._stats.last_ingest_unix_s,
            )


class TraceIngestServicer(TraceServiceServicer):
    def __init__(self, stats: StatsStore) -> None:
        self._stats = stats

    def Export(self, request, context) -> ExportTraceServiceResponse:  # type: ignore[override]
        # MVP keeps short-lived span metadata in memory for graph/anomaly analysis.
        span_count = 0
        span_points: list[SpanPoint] = []
        span_to_service: dict[tuple[str, str], str] = {}
        pending_edges: list[tuple[float, str, str, float, bool, str, str]] = []
        for rs in request.resource_spans:
            service = "unknown"
            for attr in rs.resource.attributes:
                if attr.key == "service.name":
                    service = attr.value.string_value or "unknown"
                    break
            for ils in rs.scope_spans:
                for sp in ils.spans:
                    span_count += 1
                    latency_ms = max(
                        0.0, (sp.end_time_unix_nano - sp.start_time_unix_nano) / 1_000_000.0
                    )
                    is_error = int(sp.status.code) == 2
                    trace_id = sp.trace_id.hex()
                    span_id = sp.span_id.hex()
                    parent_span_id = sp.parent_span_id.hex()
                    if parent_span_id == "0" * len(parent_span_id) and len(parent_span_id) > 0:
                        parent_span_id = ""
                    start_ns = int(sp.start_time_unix_nano or 0)
                    end_ns = int(sp.end_time_unix_nano or 0)
                    start_unix_s = start_ns / 1_000_000_000.0 if start_ns else time.time() - latency_ms / 1000.0
                    at_s = end_ns / 1_000_000_000.0 if end_ns else start_unix_s + latency_ms / 1000.0
                    span_name = (sp.name or "").strip() or service
                    point = SpanPoint(
                        at_unix_s=at_s,
                        start_unix_s=start_unix_s,
                        trace_id=trace_id,
                        span_id=span_id,
                        parent_span_id=parent_span_id,
                        name=span_name,
                        service=service,
                        latency_ms=latency_ms,
                        is_error=is_error,
                    )
                    span_points.append(point)
                    span_to_service[(trace_id, span_id)] = service
                    pending_edges.append(
                        (at_s, trace_id, parent_span_id, service, latency_ms, is_error, trace_id, span_id)
                    )
        edges: list[tuple[float, str, str, float, bool]] = []
        for at_s, trace_id, parent_span_id, child_service, latency_ms, is_error, _, _ in pending_edges:
            if not parent_span_id:
                continue
            parent_service = span_to_service.get((trace_id, parent_span_id))
            if parent_service and parent_service != child_service:
                edges.append((at_s, parent_service, child_service, latency_ms, is_error))
        self._stats.add(span_count, span_points, edges)
        return ExportTraceServiceResponse()


def serve_otlp_grpc(bind: str, stats: StatsStore) -> grpc.Server:
    # gRPC Python requires an executor; passing None can lead to broken/closed connections.
    server = grpc.server(ThreadPoolExecutor(max_workers=8))
    add_TraceServiceServicer_to_server(TraceIngestServicer(stats), server)
    server.add_insecure_port(bind)
    server.start()
    return server

