from __future__ import annotations

from dataclasses import dataclass
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


@dataclass
class IngestStats:
    spans_received: int = 0
    requests_received: int = 0
    last_ingest_unix_s: float | None = None


class StatsStore:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._stats = IngestStats()

    def add(self, span_count: int) -> None:
        now = time.time()
        with self._lock:
            self._stats.requests_received += 1
            self._stats.spans_received += span_count
            self._stats.last_ingest_unix_s = now

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
        # Count spans without storing raw spans (MVP: aggregated signal only)
        span_count = 0
        for rs in request.resource_spans:
            for ils in rs.scope_spans:
                span_count += len(ils.spans)
        self._stats.add(span_count)
        return ExportTraceServiceResponse()


def serve_otlp_grpc(bind: str, stats: StatsStore) -> grpc.Server:
    server = grpc.server(None)
    add_TraceServiceServicer_to_server(TraceIngestServicer(stats), server)
    server.add_insecure_port(bind)
    server.start()
    return server

