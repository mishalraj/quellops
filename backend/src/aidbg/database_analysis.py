from __future__ import annotations

import statistics
from collections import defaultdict
from typing import Any

LOW_IS_BAD = {"free_storage_bytes", "cache_hit_ratio", "availability"}


def apply_baseline(signal: dict[str, Any], history: list[dict[str, Any]]) -> dict[str, Any]:
    """Add a robust historical baseline without weakening provider severity."""
    result = {**signal, "evidence": dict(signal.get("evidence") or {})}
    value = result.get("value")
    if not isinstance(value, (int, float)):
        return result
    values = [
        float(item["value"])
        for item in history
        if item.get("signal") == result.get("signal")
        and isinstance(item.get("value"), (int, float))
    ][:60]
    if len(values) < 10:
        result["evidence"]["baseline_status"] = "learning"
        result["evidence"]["baseline_samples"] = len(values)
        return result
    median = statistics.median(values)
    deviations = [abs(item - median) for item in values]
    mad = statistics.median(deviations)
    scale = max(mad * 1.4826, abs(median) * 0.05, 1e-9)
    signed_score = (float(value) - median) / scale
    if result.get("signal") in LOW_IS_BAD:
        signed_score *= -1
    relative_change = abs(float(value) - median) / max(abs(median), 1.0)
    anomalous = signed_score >= 3 and relative_change >= 0.20
    critical = signed_score >= 6 and relative_change >= 0.50
    result["evidence"].update(
        {
            "baseline_status": "ready",
            "baseline_samples": len(values),
            "baseline_median": round(median, 6),
            "baseline_mad": round(mad, 6),
            "baseline_score": round(signed_score, 2),
            "relative_change": round(relative_change, 4),
            "historical_anomaly": anomalous,
        }
    )
    severity_order = {"info": 0, "warning": 1, "critical": 2}
    inferred = "critical" if critical else "warning" if anomalous else "info"
    if severity_order.get(inferred, 0) > severity_order.get(str(result.get("severity")), 0):
        result["severity"] = inferred
    return result


def history_series(signals: list[dict[str, Any]], max_points: int = 120) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for item in reversed(signals):
        if isinstance(item.get("value"), (int, float)):
            grouped[(str(item.get("source")), str(item.get("signal")), str(item.get("unit")))].append(item)
    output = []
    for (source, metric, unit), items in sorted(grouped.items()):
        items = items[-max_points:]
        values = [float(item["value"]) for item in items]
        output.append(
            {
                "source": source,
                "metric": metric,
                "unit": unit,
                "points": [
                    {"at_unix_ms": int(item["observed_at_unix_ms"]), "value": float(item["value"]), "severity": item.get("severity", "info")}
                    for item in items
                ],
                "baseline": statistics.median(values[:-1] or values),
                "current": values[-1],
                "anomaly": bool((items[-1].get("evidence") or {}).get("historical_anomaly")),
            }
        )
    return output
