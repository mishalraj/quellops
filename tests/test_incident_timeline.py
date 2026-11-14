from __future__ import annotations

import time
import unittest

from aidbg.engine import (
    ChangeEvent,
    DatabaseEvidence,
    K8sSignal,
    LogSignal,
    SignalStore,
    SpanPoint,
)


class IncidentTimelineTests(unittest.TestCase):
    def test_combines_modules_with_service_and_correlation_reasons(self):
        store = SignalStore()
        now = time.time()
        baseline = [
            SpanPoint(
                now - 400 + index,
                now - 400 + index - 0.01,
                f"base-{index}",
                f"base-span-{index}",
                "",
                "request",
                "checkout",
                10,
                False,
            )
            for index in range(20)
        ]
        current = [
            SpanPoint(
                now - 20 + index / 10,
                now - 20 + index / 10 - 0.2,
                f"cur-{index}",
                f"cur-span-{index}",
                "",
                "request",
                "checkout",
                200,
                True,
            )
            for index in range(20)
        ]
        store.add_spans(baseline + current, [])
        now_ms = int(now * 1000)
        store.add_k8s_event(
            K8sSignal(
                "k8s.pod.warning",
                now_ms - 15_000,
                "prod",
                "app",
                "checkout",
                "Pod",
                "Unhealthy",
                "Readiness probe failed",
                {},
                {},
            )
        )
        store.add_log_signal(
            LogSignal(
                "prod-logs",
                "loki",
                "checkout",
                "analysis",
                now_ms - 10_000,
                now - 70,
                now - 10,
                12,
                2,
                1,
                1000,
                ["timeout_pattern"],
                [],
                {},
            )
        )
        store.add_database_signal(
            DatabaseEvidence(
                "orders",
                "aws",
                "rds-postgresql",
                "checkout",
                "orders-ro",
                "orders",
                "replica_lag",
                "critical",
                90,
                "seconds",
                now_ms - 5_000,
                {"historical_anomaly": True},
            )
        )
        store.add_change_event(ChangeEvent("code.push", now_ms - 30_000, "checkout", "app", {}))

        incident = store.build_incident(300, 900)
        timeline = incident["timeline"]
        self.assertEqual(
            {item["module"] for item in timeline},
            {"traces", "kubernetes", "logs", "database", "changes"},
        )
        self.assertTrue(all(item["service"] == "checkout" for item in timeline))
        self.assertTrue(
            all("within_incident_window" in item["correlation_reasons"] for item in timeline)
        )
        self.assertTrue(
            any("same_service_as_trace_anomaly" in item["correlation_reasons"] for item in timeline)
        )
        self.assertEqual(
            [item["at_unix_ms"] for item in timeline],
            sorted(item["at_unix_ms"] for item in timeline),
        )


if __name__ == "__main__":
    unittest.main()
