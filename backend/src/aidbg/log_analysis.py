from __future__ import annotations

import re
import statistics
import threading
import time
from dataclasses import asdict
from typing import Any

from .engine import LogSignal, SignalStore
from .log_client import LogClient
from .log_store import LogSourceStore


ERROR_PATTERN = re.compile(
    r"\b(error|exception|timeout|timed out|failed|failure|fatal|panic|denied|unavailable|refused)\b",
    re.IGNORECASE,
)
SIGNAL_PATTERNS = {
    "timeout_pattern": re.compile(r"\b(timeout|timed out|deadline exceeded)\b", re.I),
    "connection_pattern": re.compile(r"\b(connection (refused|reset)|unavailable|broken pipe|no route to host)\b", re.I),
    "authentication_pattern": re.compile(r"\b(unauthorized|forbidden|permission denied|invalid (token|credential)|access denied)\b", re.I),
    "rate_limit_pattern": re.compile(r"\b(rate.?limit|too many requests|throttl(ed|ing)|http 429)\b", re.I),
    "resource_exhaustion_pattern": re.compile(r"\b(out of memory|oomkilled|oom killing|disk full|no space left|resource exhausted)\b", re.I),
    "database_pattern": re.compile(r"\b(deadlock|connection pool exhausted|too many connections|lock wait timeout|query cancelled)\b", re.I),
    "crash_pattern": re.compile(r"\b(panic|segmentation fault|crashloopbackoff|fatal|uncaught exception)\b", re.I),
}
FINGERPRINT_NOISE = re.compile(
    r"\b(?:[0-9a-f]{8}-[0-9a-f-]{27,}|0x[0-9a-f]+|\d+(?:\.\d+)*)\b", re.I
)


class LogSignalAnalyzer:
    """Turns watcher runs into bounded, deterministic incident evidence."""

    def __init__(
        self,
        signals: SignalStore,
        client: LogClient,
        *,
        investigation_cooldown_s: int = 300,
        history_store: LogSourceStore | None = None,
    ):
        self.signals = signals
        self.client = client
        self.investigation_cooldown_s = investigation_cooldown_s
        self.history_store = history_store
        self._last_investigated: dict[str, float] = {}
        self._lock = threading.Lock()

    def _record(self, signal: LogSignal) -> None:
        self.signals.add_log_signal(signal)
        if self.history_store is not None:
            self.history_store.add_detection(asdict(signal))

    @staticmethod
    def _samples(records: list[dict[str, Any]], limit: int = 5) -> list[dict[str, Any]]:
        samples: list[dict[str, Any]] = []
        seen: set[str] = set()
        for record in records:
            message = str(record.get("message") or "")[:500]
            if not message or message in seen:
                continue
            seen.add(message)
            samples.append(
                {
                    "id": str(record.get("id") or ""),
                    "timestamp": record.get("timestamp") or record.get("timestamp_unix_s"),
                    "message": message,
                }
            )
            if len(samples) >= limit:
                break
        return samples

    @staticmethod
    def _focus(records: list[dict[str, Any]]) -> str:
        text = "\n".join(str(record.get("message") or "") for record in records)
        if re.search(r"\b(timeout|timed out|deadline exceeded)\b", text, re.IGNORECASE):
            return "timeout"
        if re.search(r"\b(connection refused|connection reset|unavailable)\b", text, re.IGNORECASE):
            return "refused"
        if ERROR_PATTERN.search(text):
            return "error"
        return ""

    @staticmethod
    def _fingerprint(message: str) -> str:
        return " ".join(FINGERPRINT_NOISE.sub("?", message.lower()).split())[:300]

    def _baseline_records(self, source: str) -> float:
        if self.history_store is None:
            return 0.0
        recent = self.history_store.list_detections(source, limit=20)
        counts = [
            int(item.get("records") or 0)
            for item in recent
            if item.get("kind") == "watcher_run" and not item.get("error")
        ]
        return float(statistics.median(counts)) if counts else 0.0

    def observe(self, batch: dict[str, Any]) -> None:
        now = time.time()
        source = str(batch.get("source") or "")
        provider = str(batch.get("provider") or "")
        service = str(batch.get("service") or "")
        start = float(batch.get("window_start_unix_s") or now)
        end = float(batch.get("window_end_unix_s") or now)
        previous = int(batch.get("previous_records") or 0)
        records = list(batch.get("records") or [])
        count = len(records)

        if not batch.get("ok"):
            self._record(
                LogSignal(
                    source=source,
                    provider=provider,
                    service=service,
                    kind="watcher_failure",
                    at_unix_ms=int(now * 1000),
                    window_start_unix_s=start,
                    window_end_unix_s=end,
                    records=0,
                    previous_records=previous,
                    requests=0,
                    bytes_received=0,
                    reasons=[],
                    samples=[],
                    investigation={},
                    error=str(batch.get("error") or "log watcher failed")[:500],
                )
            )
            return

        stale_catchup = now - end > 300
        reasons: list[str] = []
        if not stale_catchup:
            baseline = self._baseline_records(source)
            if count >= 5 and baseline == 0:
                reasons.append("matching_log_volume")
            if baseline > 0 and count >= max(5, baseline * 3):
                reasons.append("volume_anomaly")
            if previous > 0 and count >= max(previous * 2, previous + 3):
                reasons.append("record_spike")
            messages = [str(record.get("message") or "") for record in records]
            if any(ERROR_PATTERN.search(message) for message in messages):
                reasons.append("error_pattern")
            for reason, pattern in SIGNAL_PATTERNS.items():
                if any(pattern.search(message) for message in messages):
                    reasons.append(reason)
            fingerprints: dict[str, int] = {}
            for message in messages:
                fingerprint = self._fingerprint(message)
                if fingerprint:
                    fingerprints[fingerprint] = fingerprints.get(fingerprint, 0) + 1
            if fingerprints and max(fingerprints.values()) >= 3:
                reasons.append("repeated_pattern")
            reasons = list(dict.fromkeys(reasons))

        samples = self._samples(records)
        investigation: dict[str, Any] = {}
        if reasons:
            with self._lock:
                last = self._last_investigated.get(source, 0.0)
                allowed = now - last >= self.investigation_cooldown_s
                if allowed:
                    self._last_investigated[source] = now
            if allowed:
                try:
                    scope_s = min(int(batch.get("max_scope_s") or 900), 900)
                    target = min(100, max(10, count))
                    focus = self._focus(records)
                    result = self.client.investigate(
                        source, max(0.0, end - scope_s), end, target, focus
                    )
                    usage = result.get("usage") or {}
                    investigation = {
                        "target_reached": bool(result.get("target_reached")),
                        "scope_start": result.get("scope_start"),
                        "scope_end": result.get("scope_end"),
                        "stop_reason": result.get("stop_reason"),
                        "backend_requests": int(usage.get("backend_requests", 0)),
                        "bytes_received": int(usage.get("bytes_received", 0)),
                        "warnings": list(result.get("warnings") or [])[:5],
                        "focus": focus,
                    }
                    investigated_samples = self._samples(list(result.get("records") or []))
                    if investigated_samples:
                        samples = investigated_samples
                except Exception as exc:
                    investigation = {"error": str(exc)[:500]}
            else:
                investigation = {"skipped": "cooldown"}

        self._record(
            LogSignal(
                source=source,
                provider=provider,
                service=service,
                kind="log_catchup" if stale_catchup else "watcher_run",
                at_unix_ms=int(now * 1000),
                window_start_unix_s=start,
                window_end_unix_s=end,
                records=count,
                previous_records=previous,
                requests=int(batch.get("requests") or 0),
                bytes_received=int(batch.get("bytes_received") or 0),
                reasons=reasons,
                samples=samples if reasons else [],
                investigation=investigation,
            )
        )
