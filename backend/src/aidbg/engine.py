from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import asdict, dataclass
import threading
import time
from typing import Any


def _pct(values: list[float], percentile: float) -> float:
    if not values:
        return 0.0
    if len(values) == 1:
        return values[0]
    values = sorted(values)
    idx = int((len(values) - 1) * percentile)
    return values[idx]


def _event_suggests_recent_deploy(ev_type: str) -> bool:
    """Match agent event types: k8s.deployment.*, deploy.started|completed|failed, rollout strings."""
    tl = str(ev_type).lower()
    if "deployment" in tl:
        return True
    if tl.startswith("deploy."):
        return True
    if "rollout" in tl:
        return True
    return False


def _pick_top_causes(candidates: list[dict[str, Any]], k: int = 3) -> list[dict[str, Any]]:
    """Prefer a mix of cause types: at most two traffic_spike rows, then backfill by score."""
    if not candidates:
        return []
    sorted_c = sorted(candidates, key=lambda x: x["score"], reverse=True)
    chosen: list[dict[str, Any]] = []
    chosen_idx: set[int] = set()
    traffic_rows = 0
    for idx, c in enumerate(sorted_c):
        if len(chosen) >= k:
            break
        if c.get("type") == "traffic_spike" and traffic_rows >= 2:
            continue
        chosen.append(c)
        chosen_idx.add(idx)
        if c.get("type") == "traffic_spike":
            traffic_rows += 1
    for idx, c in enumerate(sorted_c):
        if len(chosen) >= k:
            break
        if idx in chosen_idx:
            continue
        chosen.append(c)
        chosen_idx.add(idx)
    return chosen[:k]


@dataclass
class SpanPoint:
    at_unix_s: float
    start_unix_s: float
    trace_id: str
    span_id: str
    parent_span_id: str
    name: str
    service: str
    latency_ms: float
    is_error: bool


@dataclass
class K8sSignal:
    type: str
    at_unix_ms: int
    cluster: str | None
    namespace: str | None
    name: str | None
    kind: str | None
    reason: str | None
    message: str | None
    labels: dict[str, str]
    fields: dict[str, str]


class SignalStore:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._spans: deque[SpanPoint] = deque()
        self._edges: deque[tuple[float, str, str, float, bool]] = deque()
        self._k8s_events: deque[K8sSignal] = deque()

    def _trim(self, now_s: float, horizon_s: int = 3600) -> None:
        cutoff = now_s - horizon_s
        while self._spans and self._spans[0].at_unix_s < cutoff:
            self._spans.popleft()
        while self._edges and self._edges[0][0] < cutoff:
            self._edges.popleft()
        cutoff_ms = int(cutoff * 1000)
        while self._k8s_events and self._k8s_events[0].at_unix_ms < cutoff_ms:
            self._k8s_events.popleft()

    def add_spans(
        self, span_points: list[SpanPoint], edges: list[tuple[float, str, str, float, bool]]
    ) -> None:
        now_s = time.time()
        with self._lock:
            self._spans.extend(span_points)
            self._edges.extend(edges)
            self._trim(now_s)

    def add_k8s_event(self, event: K8sSignal) -> None:
        now_s = time.time()
        with self._lock:
            self._k8s_events.append(event)
            self._trim(now_s)

    def recent_k8s_events(self, limit: int = 100) -> list[dict[str, Any]]:
        with self._lock:
            items = list(self._k8s_events)[-limit:]
        return [asdict(i) for i in items]

    def service_metrics(self, window_s: int) -> list[dict[str, Any]]:
        now_s = time.time()
        cutoff = now_s - window_s
        by_service: dict[str, list[SpanPoint]] = defaultdict(list)
        with self._lock:
            for p in self._spans:
                if p.at_unix_s >= cutoff:
                    by_service[p.service].append(p)
        out: list[dict[str, Any]] = []
        for service, points in by_service.items():
            latencies = [p.latency_ms for p in points]
            errors = sum(1 for p in points if p.is_error)
            out.append(
                {
                    "service": service,
                    "request_count": len(points),
                    "error_rate": (errors / len(points)) if points else 0.0,
                    "p50_latency_ms": _pct(latencies, 0.50),
                    "p95_latency_ms": _pct(latencies, 0.95),
                }
            )
        out.sort(key=lambda x: x["service"])
        return out

    def graph_snapshot(self, window_s: int) -> dict[str, Any]:
        now_s = time.time()
        cutoff = now_s - window_s
        node_stats: dict[str, dict[str, float]] = defaultdict(
            lambda: {"request_count": 0.0, "error_count": 0.0}
        )
        edge_map: dict[tuple[str, str], list[tuple[float, bool]]] = defaultdict(list)
        with self._lock:
            for p in self._spans:
                if p.at_unix_s >= cutoff:
                    node_stats[p.service]["request_count"] += 1
                    if p.is_error:
                        node_stats[p.service]["error_count"] += 1
            for at_s, src, dst, latency_ms, is_error in self._edges:
                if at_s >= cutoff:
                    edge_map[(src, dst)].append((latency_ms, is_error))
        nodes = [
            {
                "id": service,
                "request_count": int(stats["request_count"]),
                "error_rate": (stats["error_count"] / stats["request_count"])
                if stats["request_count"]
                else 0.0,
            }
            for service, stats in sorted(node_stats.items())
        ]
        edges = []
        for (src, dst), vals in sorted(edge_map.items()):
            lats = [lat for lat, _ in vals]
            errors = sum(1 for _, err in vals if err)
            edges.append(
                {
                    "source": src,
                    "target": dst,
                    "request_count": len(vals),
                    "error_rate": errors / len(vals),
                    "p95_latency_ms": _pct(lats, 0.95),
                }
            )
        return {"window_s": window_s, "nodes": nodes, "edges": edges}

    def trace_focus_snapshot(self, impacted_service: str, window_s: int, max_spans: int = 96) -> dict[str, Any] | None:
        """Pick one recent trace touching `impacted_service`, return span rows for a Jaeger-style Gantt (subtree)."""
        target = (impacted_service or "").strip()
        if not target:
            return None
        now_s = time.time()
        cutoff = now_s - window_s
        with self._lock:
            by_trace: dict[str, list[SpanPoint]] = defaultdict(list)
            for p in self._spans:
                if p.at_unix_s >= cutoff:
                    by_trace[p.trace_id].append(p)
        candidates: list[tuple[bool, float, float, str, list[SpanPoint]]] = []
        for tid, pts in by_trace.items():
            if not any(p.service == target for p in pts):
                continue
            err_hit = any(p.service == target and p.is_error for p in pts)
            lat_hit = max((p.latency_ms for p in pts if p.service == target), default=0.0)
            last_end = max(p.at_unix_s for p in pts)
            candidates.append((err_hit, lat_hit, last_end, tid, pts))
        if not candidates:
            return None
        candidates.sort(key=lambda x: (-int(x[0]), -x[1], -x[2]))
        _, _, _, trace_id, pts = candidates[0]
        by_id = {p.span_id: p for p in pts}

        def subtree_span_ids() -> set[str]:
            hot = {p.span_id for p in pts if p.service == target}
            if not hot:
                return {p.span_id for p in pts}
            keep: set[str] = set(hot)
            for sid in list(hot):
                cur = sid
                for _ in range(128):
                    p = by_id.get(cur)
                    if not p or not p.parent_span_id:
                        break
                    pid = p.parent_span_id
                    if pid in by_id:
                        keep.add(pid)
                    cur = pid
            stack = list(hot)
            while stack:
                sid = stack.pop()
                for p in pts:
                    if p.parent_span_id == sid and p.span_id not in keep:
                        keep.add(p.span_id)
                        stack.append(p.span_id)
            return keep

        keep_ids = subtree_span_ids()
        filtered = [p for p in pts if p.span_id in keep_ids]
        trace_start = min(p.start_unix_s for p in filtered)
        trace_end = max(p.at_unix_s for p in filtered)
        duration_ms = max((trace_end - trace_start) * 1000.0, 0.001)

        def depth_of(sid: str) -> int:
            d = 0
            cur = sid
            seen: set[str] = set()
            for _ in range(128):
                p = by_id.get(cur)
                if not p or not p.parent_span_id:
                    break
                pid = p.parent_span_id
                if pid not in by_id or pid in seen:
                    break
                seen.add(cur)
                d += 1
                cur = pid
            return d

        rows: list[dict[str, Any]] = []
        for p in sorted(filtered, key=lambda x: (x.start_unix_s, x.span_id)):
            start_ms = max(0.0, (p.start_unix_s - trace_start) * 1000.0)
            rows.append(
                {
                    "span_id": p.span_id,
                    "parent_span_id": p.parent_span_id,
                    "name": p.name or p.service or "span",
                    "service": p.service,
                    "start_ms": round(start_ms, 3),
                    "duration_ms": round(p.latency_ms, 3),
                    "is_error": p.is_error,
                    "impacted": p.service == target,
                    "depth": depth_of(p.span_id),
                }
            )
        truncated = False
        if len(rows) > max_spans:
            rows = rows[:max_spans]
            truncated = True
        return {
            "trace_id": trace_id,
            "duration_ms": round(duration_ms, 3),
            "impacted_service": target,
            "trace_start_unix_s": trace_start,
            "subtree_only": True,
            "span_count_total": len(filtered),
            "truncated": truncated,
            "spans": rows,
        }

    def detect_anomalies(self, current_window_s: int, baseline_window_s: int) -> list[dict[str, Any]]:
        now_s = time.time()
        cur_start = now_s - current_window_s
        base_start = cur_start - baseline_window_s
        current_by_service: dict[str, list[SpanPoint]] = defaultdict(list)
        baseline_by_service: dict[str, list[SpanPoint]] = defaultdict(list)
        with self._lock:
            for p in self._spans:
                if p.at_unix_s >= cur_start:
                    current_by_service[p.service].append(p)
                elif p.at_unix_s >= base_start:
                    baseline_by_service[p.service].append(p)
        anomalies: list[dict[str, Any]] = []
        for service, cur_points in current_by_service.items():
            base_points = baseline_by_service.get(service, [])
            if len(cur_points) < 20 or len(base_points) < 20:
                continue
            cur_lats = [p.latency_ms for p in cur_points]
            base_lats = [p.latency_ms for p in base_points]
            cur_p95 = _pct(cur_lats, 0.95)
            base_p95 = max(_pct(base_lats, 0.95), 1.0)
            cur_err = sum(1 for p in cur_points if p.is_error) / len(cur_points)
            base_err = sum(1 for p in base_points if p.is_error) / len(base_points)
            lat_ratio = cur_p95 / base_p95
            if lat_ratio >= 1.5:
                anomalies.append(
                    {
                        "kind": "latency_spike",
                        "entity": service,
                        "current_p95_ms": cur_p95,
                        "baseline_p95_ms": base_p95,
                        "magnitude": lat_ratio,
                        "start_unix_s": cur_start,
                    }
                )
            err_delta = cur_err - base_err
            if err_delta >= 0.05:
                anomalies.append(
                    {
                        "kind": "error_spike",
                        "entity": service,
                        "current_error_rate": cur_err,
                        "baseline_error_rate": base_err,
                        "magnitude": err_delta,
                        "start_unix_s": cur_start,
                    }
                )
        return anomalies

    def build_incident(self, current_window_s: int, baseline_window_s: int) -> dict[str, Any]:
        anomalies = self.detect_anomalies(current_window_s, baseline_window_s)
        graph = self.graph_snapshot(current_window_s)
        node_requests = {n["id"]: int(n.get("request_count", 0)) for n in graph.get("nodes", [])}
        now_ms = int(time.time() * 1000)
        recent_events = self.recent_k8s_events(limit=200)
        affected_entities = {a.get("entity", "") for a in anomalies if a.get("entity")}
        deploy_candidates: list[dict[str, Any]] = []
        for ev in reversed(recent_events):
            t = ev.get("type", "")
            if not _event_suggests_recent_deploy(str(t)):
                continue
            fields = ev.get("fields") or {}
            if isinstance(fields, dict) and str(fields.get("desired_replicas", "")) == "0":
                # Scale-to-zero / dependency outage — not a rollout hypothesis; avoid beating dependency_unavailable.
                continue
            age_s = (now_ms - int(ev.get("at_unix_ms", now_ms))) / 1000.0
            if age_s > 1800:
                continue
            name = str(ev.get("name") or "")
            evidence: dict[str, Any] = {
                "event_type": ev.get("type"),
                "age_s": round(age_s, 1),
            }
            if isinstance(fields, dict):
                if fields.get("rollout_status"):
                    evidence["rollout_status"] = fields["rollout_status"]
                if fields.get("updated_replicas") is not None:
                    evidence["updated_replicas"] = fields.get("updated_replicas")
                if fields.get("ready_replicas") is not None:
                    evidence["ready_replicas"] = fields.get("ready_replicas")
                if fields.get("desired_replicas") is not None:
                    evidence["desired_replicas"] = fields.get("desired_replicas")
            if ev.get("reason"):
                evidence["k8s_reason"] = ev.get("reason")
            if ev.get("message"):
                msg = str(ev.get("message"))
                evidence["k8s_message"] = msg if len(msg) <= 240 else msg[:237] + "..."
            score = max(0.1, 1.0 - (age_s / 1800.0))
            if str(t).lower().startswith("deploy.") and "started" in str(t).lower():
                score += 0.08
                evidence["rollout_phase"] = "in_progress"
            if name in affected_entities:
                score += 0.25
                evidence["service_match"] = True
            gr = node_requests.get(name, 0)
            if gr >= 80:
                score += 0.12
                evidence["trace_requests_in_window"] = gr
            deploy_candidates.append(
                {
                    "type": "recent_deploy",
                    "entity": name,
                    "namespace": ev.get("namespace"),
                    "score": min(score, 1.2),
                    "evidence": evidence,
                }
            )
            if len(deploy_candidates) >= 5:
                break
        _dedup_deploy: dict[tuple[str, str], dict[str, Any]] = {}
        for c in deploy_candidates:
            k = (str(c.get("entity") or ""), str(c.get("namespace") or ""))
            if k not in _dedup_deploy or float(c["score"]) > float(_dedup_deploy[k]["score"]):
                _dedup_deploy[k] = c
        deploy_candidates = list(_dedup_deploy.values())
        dependency_down_candidates: list[dict[str, Any]] = []
        for ev in reversed(recent_events):
            t = str(ev.get("type", ""))
            fields = ev.get("fields", {}) or {}
            desired_replicas = str(fields.get("desired_replicas", ""))
            if desired_replicas == "0":
                age_s = (now_ms - int(ev.get("at_unix_ms", now_ms))) / 1000.0
                if age_s <= 1800:
                    dependency_down_candidates.append(
                        {
                            "type": "dependency_unavailable",
                            "entity": ev.get("name"),
                            "namespace": ev.get("namespace"),
                            # Above recent_deploy cap (1.2) when fresh so scale-to-zero wins top-1.
                            "score": max(0.5, 1.32 - (age_s / 2400.0)),
                            "evidence": {
                                "event_type": t,
                                "desired_replicas": desired_replicas,
                                "updated_replicas": fields.get("updated_replicas"),
                                "ready_replicas": fields.get("ready_replicas"),
                            },
                        }
                    )
                    if len(dependency_down_candidates) >= 5:
                        break
                continue
            if "pod" not in t and "event" not in t:
                continue
            msg = str(ev.get("message", "")).lower()
            reason = str(ev.get("reason", "")).lower()
            if "back-off" not in msg and "crashloop" not in msg and "unhealthy" not in reason:
                continue
            age_s = (now_ms - int(ev.get("at_unix_ms", now_ms))) / 1000.0
            if age_s > 1800:
                continue
            dependency_down_candidates.append(
                {
                    "type": "dependency_unavailable",
                    "entity": ev.get("fields", {}).get("involved_name") or ev.get("name"),
                    "namespace": ev.get("namespace"),
                    "score": max(0.2, 0.9 - (age_s / 2400.0)),
                    "evidence": {"event_type": t, "reason": ev.get("reason"), "message": ev.get("message")},
                }
            )
            if len(dependency_down_candidates) >= 5:
                break
        _dedup_dep: dict[tuple[str, str], dict[str, Any]] = {}
        for c in dependency_down_candidates:
            dk = (str(c.get("entity") or ""), str(c.get("namespace") or ""))
            if dk not in _dedup_dep or float(c["score"]) > float(_dedup_dep[dk]["score"]):
                _dedup_dep[dk] = c
        dependency_down_candidates = list(_dedup_dep.values())
        traffic_candidates: list[dict[str, Any]] = []
        metrics = self.service_metrics(current_window_s)
        # OTel demo light: per-service span counts stay moderate; keep threshold reachable.
        # Ceiling must exceed typical fresh recent_deploy (~1.0–1.1) so load-spike scenarios rank correctly.
        min_requests = 380
        for m in metrics:
            if m["request_count"] < min_requests or m["error_rate"] >= 0.01:
                continue
            rc = float(m["request_count"])
            excess = max(0.0, rc - min_requests)
            score = min(1.26, 0.46 + min(1.0, excess / 1900.0) * 0.80)
            traffic_candidates.append(
                {
                    "type": "traffic_spike",
                    "entity": m["service"],
                    "score": score,
                    "evidence": {
                        "request_count": int(rc),
                        "p95_latency_ms": round(m["p95_latency_ms"], 2),
                        "error_rate": round(m["error_rate"], 4),
                    },
                }
            )
        candidates = deploy_candidates + dependency_down_candidates + traffic_candidates
        candidates.sort(key=lambda x: x["score"], reverse=True)
        if anomalies and not candidates:
            # Keep incident deterministic even when no strong candidate exists yet.
            candidates.append(
                {
                    "type": "unknown_regression",
                    "entity": anomalies[0].get("entity"),
                    "score": 0.2,
                    "evidence": {"reason": "anomaly_detected_without_matching_k8s_signal"},
                }
            )
        top_causes = _pick_top_causes(candidates, 3)
        return {
            "generated_at_unix_ms": now_ms,
            "anomalies": anomalies,
            "top_causes": top_causes,
            "graph_snapshot": graph,
        }

    def why_service_slow(self, service: str, current_window_s: int, baseline_window_s: int) -> dict[str, Any]:
        incident = self.build_incident(current_window_s, baseline_window_s)
        service_anoms = [a for a in incident["anomalies"] if a.get("entity") == service]
        return {
            "service": service,
            "incident_generated_at_unix_ms": incident["generated_at_unix_ms"],
            "anomalies": service_anoms,
            "top_causes": incident["top_causes"],
        }

    def evidence_bundle(self, service: str, current_window_s: int, baseline_window_s: int) -> dict[str, Any]:
        metrics = self.service_metrics(current_window_s)
        metric = next((m for m in metrics if m["service"] == service), None)
        graph = self.graph_snapshot(current_window_s)
        relevant_edges = [
            e for e in graph["edges"] if e["source"] == service or e["target"] == service
        ]
        anomalies = self.detect_anomalies(current_window_s, baseline_window_s)
        return {
            "service": service,
            "metrics": metric,
            "edges": relevant_edges,
            "anomalies": [a for a in anomalies if a.get("entity") == service],
        }
