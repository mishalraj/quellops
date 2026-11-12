import base64
import os
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import Mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "backend" / "src"))

from aidbg.engine import LogSignal, SignalStore
from aidbg.log_analysis import LogSignalAnalyzer
from aidbg.log_store import LogSourceStore


def records(count: int, message: str) -> list[dict]:
    return [
        {"id": str(index), "timestamp": "2026-11-10T00:00:00Z", "message": message}
        for index in range(count)
    ]


class LogSignalAnalyzerTests(unittest.TestCase):
    def setUp(self):
        self.signals = SignalStore()
        self.client = Mock()
        self.client.investigate.return_value = {
            "target_reached": True,
            "scope_start": "2026-11-09T23:59:00Z",
            "scope_end": "2026-11-10T00:00:00Z",
            "stop_reason": "target_reached",
            "records": records(2, "database timeout while checking out"),
            "usage": {"backend_requests": 2, "bytes_received": 100},
        }
        self.analyzer = LogSignalAnalyzer(self.signals, self.client)

    def test_suspicious_batch_triggers_bounded_investigation(self):
        now = time.time()
        self.analyzer.observe(
            {
                "ok": True,
                "source": "prod",
                "provider": "loki",
                "service": "checkout",
                "window_start_unix_s": now - 30,
                "window_end_unix_s": now,
                "records": records(5, "request failed with timeout"),
                "previous_records": 1,
                "requests": 1,
                "bytes_received": 50,
                "max_scope_s": 3600,
            }
        )
        self.client.investigate.assert_called_once()
        args = self.client.investigate.call_args.args
        self.assertEqual(args[0], "prod")
        self.assertLessEqual(args[2] - args[1], 900)
        self.assertEqual(args[4], "timeout")
        signal = self.signals.recent_log_signals()[-1]
        self.assertIn("error_pattern", signal["reasons"])
        self.assertIn("record_spike", signal["reasons"])
        self.assertEqual(signal["samples"][0]["message"], "database timeout while checking out")

    def test_quiet_batch_does_not_investigate(self):
        now = time.time()
        self.analyzer.observe(
            {
                "ok": True,
                "source": "prod",
                "provider": "loki",
                "window_start_unix_s": now - 30,
                "window_end_unix_s": now,
                "records": records(1, "request completed"),
            }
        )
        self.client.investigate.assert_not_called()
        self.assertEqual(self.signals.recent_log_signals()[-1]["reasons"], [])

    def test_specific_failure_classes_and_repeated_patterns_are_detected(self):
        now = time.time()
        self.analyzer.observe(
            {
                "ok": True,
                "source": "prod",
                "provider": "loki",
                "window_start_unix_s": now - 30,
                "window_end_unix_s": now,
                "records": records(3, "HTTP 429 rate limit exceeded for request 123"),
            }
        )
        signal = self.signals.recent_log_signals()[-1]
        self.assertIn("rate_limit_pattern", signal["reasons"])
        self.assertIn("repeated_pattern", signal["reasons"])

    def test_old_catchup_batch_cannot_trigger_current_incident(self):
        now = time.time()
        self.analyzer.observe(
            {
                "ok": True,
                "source": "prod",
                "provider": "loki",
                "window_start_unix_s": now - 1000,
                "window_end_unix_s": now - 600,
                "records": records(20, "fatal exception"),
            }
        )
        self.client.investigate.assert_not_called()
        signal = self.signals.recent_log_signals()[-1]
        self.assertEqual(signal["kind"], "log_catchup")
        self.assertEqual(signal["reasons"], [])

    def test_failure_is_recorded_without_provider_call(self):
        self.analyzer.observe(
            {
                "ok": False,
                "source": "prod",
                "provider": "loki",
                "service": "checkout",
                "error": "provider unavailable",
            }
        )
        self.client.investigate.assert_not_called()
        signal = self.signals.recent_log_signals()[-1]
        self.assertEqual(signal["kind"], "watcher_failure")
        self.assertEqual(signal["error"], "provider unavailable")

    def test_detection_history_survives_a_new_store_instance(self):
        with tempfile.TemporaryDirectory() as directory:
            key = base64.b64encode(os.urandom(32)).decode("ascii")
            path = str(Path(directory) / "control.db")
            history = LogSourceStore(path, key)
            history.initialize()
            analyzer = LogSignalAnalyzer(self.signals, self.client, history_store=history)
            analyzer.observe(
                {
                    "ok": False,
                    "source": "prod",
                    "provider": "loki",
                    "error": "provider unavailable",
                }
            )
            reopened = LogSourceStore(path, key)
            reopened.initialize()
            saved = reopened.list_detections("prod")
            self.assertEqual(len(saved), 1)
            self.assertEqual(saved[0]["kind"], "watcher_failure")


class LogIncidentTests(unittest.TestCase):
    def signal(self, kind="watcher_run", reasons=None, error=None):
        now = time.time()
        return LogSignal(
            source="prod",
            provider="loki",
            service="checkout",
            kind=kind,
            at_unix_ms=int(now * 1000),
            window_start_unix_s=now - 30,
            window_end_unix_s=now,
            records=5,
            previous_records=1,
            requests=1,
            bytes_received=100,
            reasons=reasons or [],
            samples=[{"message": "request failed"}] if reasons else [],
            investigation={},
            error=error,
        )

    def test_log_signal_is_ranked_and_exposed_as_evidence(self):
        store = SignalStore()
        store.add_log_signal(self.signal(reasons=["error_pattern"]))
        incident = store.build_incident(300, 900)
        self.assertEqual(incident["top_causes"][0]["type"], "log_error_pattern")
        self.assertEqual(incident["log_evidence"][0]["source"], "prod")
        self.assertEqual(
            store.evidence_bundle("checkout", 300, 900)["log_evidence"][0]["source"], "prod"
        )

    def test_only_consecutive_failures_create_unavailable_cause(self):
        store = SignalStore()
        store.add_log_signal(self.signal(kind="watcher_failure", error="down"))
        store.add_log_signal(self.signal())
        store.add_log_signal(self.signal(kind="watcher_failure", error="down"))
        store.add_log_signal(self.signal(kind="watcher_failure", error="down"))
        self.assertEqual(store.log_signal_candidates(int(time.time() * 1000), set()), [])
        store.add_log_signal(self.signal(kind="watcher_failure", error="down"))
        candidates = store.log_signal_candidates(int(time.time() * 1000), set())
        self.assertEqual(candidates[0]["type"], "log_source_unavailable")
        self.assertEqual(candidates[0]["evidence"]["consecutive_recent_failures"], 3)


if __name__ == "__main__":
    unittest.main()
