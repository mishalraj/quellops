import base64
import os
import sys
import tempfile
import unittest
import random
from pathlib import Path
from unittest.mock import Mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "backend" / "src"))

from aidbg.log_client import LogClientError
from aidbg.log_store import LogSourceStore
from aidbg.log_watcher import LogWatcher


class LogWatcherTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        key = base64.b64encode(os.urandom(32)).decode("ascii")
        self.store = LogSourceStore(
            str(Path(self.directory.name) / "control.db"), key
        )
        self.store.initialize()
        self.store.upsert(
            "prod",
            "loki",
            {
                "url": "https://loki.example.com",
                "query": "{app=\"api\"}",
                "max_scope_s": 60,
                "estimated_cost_per_1000_requests_usd": 5,
            },
            {},
        )
        self.store.upsert_watcher(
            "prod",
            enabled=True,
            interval_s=30,
            lag_s=5,
            initial_lookback_s=120,
        )

    def tearDown(self):
        self.directory.cleanup()

    def test_checkpoint_advances_only_through_complete_bounded_chunk(self):
        client = Mock()
        observer = Mock()
        client.fetch_complete.return_value = {
            "coverage_complete": True,
            "records": [{"id": "1"}, {"id": "2"}],
            "usage": {"backend_requests": 3, "bytes_received": 400},
        }
        result = LogWatcher(self.store, client, observer, random_source=random.Random(0)).run_once("prod", now=1000)
        self.assertTrue(result["ok"])
        self.assertFalse(result["caught_up"])
        client.fetch_complete.assert_called_once_with("prod", 875, 935)
        saved = self.store.get_watcher("prod")
        self.assertEqual(saved["checkpoint_unix_s"], 935)
        self.assertEqual(saved["next_run_unix_s"], 1001)
        self.assertEqual(saved["last_records"], 2)
        self.assertEqual(saved["last_requests"], 3)
        self.assertEqual(saved["last_bytes"], 400)
        self.assertEqual(saved["total_runs"], 1)
        self.assertEqual(saved["total_requests"], 3)
        self.assertEqual(saved["total_records"], 2)
        self.assertAlmostEqual(saved["estimated_cost_usd"], 0.015)
        batch = observer.call_args.args[0]
        self.assertEqual(batch["provider"], "loki")
        self.assertEqual(len(batch["records"]), 2)
        self.assertEqual(batch["previous_records"], 0)

    def test_failed_fetch_keeps_checkpoint(self):
        self.store.watcher_succeeded(
            "prod",
            checkpoint=900,
            completed=900,
            next_run=900,
            records=1,
            requests=1,
            bytes_received=10,
        )
        client = Mock()
        observer = Mock()
        client.fetch_complete.side_effect = LogClientError("provider unavailable")
        result = LogWatcher(self.store, client, observer, random_source=random.Random(0)).run_once("prod", now=1000)
        self.assertFalse(result["ok"])
        saved = self.store.get_watcher("prod")
        self.assertEqual(saved["checkpoint_unix_s"], 900)
        self.assertGreaterEqual(saved["next_run_unix_s"], 1030)
        self.assertLessEqual(saved["next_run_unix_s"], 1033)
        self.assertEqual(saved["last_error"], "provider unavailable")
        self.assertEqual(saved["consecutive_failures"], 1)
        self.assertFalse(observer.call_args.args[0]["ok"])

    def test_observer_failure_does_not_block_checkpoint(self):
        client = Mock()
        client.fetch_complete.return_value = {
            "coverage_complete": True,
            "records": [],
            "usage": {},
        }
        result = LogWatcher(
            self.store, client, Mock(side_effect=RuntimeError("analysis failed"))
        ).run_once("prod", now=1000)
        self.assertTrue(result["ok"])
        self.assertEqual(self.store.get_watcher("prod")["checkpoint_unix_s"], 935)

    def test_database_lease_allows_only_one_watcher_instance(self):
        first = self.store.claim_due_watchers(1000, "replica-a")
        second = self.store.claim_due_watchers(1000, "replica-b")
        self.assertEqual([item["source_name"] for item in first], ["prod"])
        self.assertEqual(second, [])

    def test_failures_back_off_and_success_resets_counter(self):
        client = Mock()
        client.fetch_complete.side_effect = LogClientError("down")
        watcher = LogWatcher(self.store, client, random_source=random.Random(0))
        watcher.run_once("prod", now=1000)
        first = self.store.get_watcher("prod")
        self.assertEqual(first["consecutive_failures"], 1)
        watcher.run_once("prod", now=1100)
        second = self.store.get_watcher("prod")
        self.assertEqual(second["consecutive_failures"], 2)
        self.assertGreaterEqual(second["next_run_unix_s"], 1160)
        client.fetch_complete.side_effect = None
        client.fetch_complete.return_value = {"coverage_complete": True, "records": [], "usage": {}}
        watcher.run_once("prod", now=1200)
        self.assertEqual(self.store.get_watcher("prod")["consecutive_failures"], 0)

    def test_throttling_and_failure_totals_are_persisted(self):
        client = Mock()
        client.fetch_complete.side_effect = LogClientError("HTTP 429 throttled")
        LogWatcher(self.store, client, random_source=random.Random(0)).run_once(
            "prod", now=1000
        )
        saved = self.store.get_watcher("prod")
        self.assertEqual(saved["total_runs"], 1)
        self.assertEqual(saved["total_failures"], 1)
        self.assertEqual(saved["total_throttles"], 1)


if __name__ == "__main__":
    unittest.main()
