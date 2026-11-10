import base64
import os
import sys
import unittest
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "backend" / "src"))

from aidbg.log_store import PostgresLogSourceStore, create_log_source_store


POSTGRES_URL = os.getenv("AIDBG_TEST_POSTGRES_URL", "")


@unittest.skipUnless(POSTGRES_URL, "AIDBG_TEST_POSTGRES_URL is not configured")
class PostgresLogStoreTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.key = base64.b64encode(os.urandom(32)).decode("ascii")
        cls.store = PostgresLogSourceStore(POSTGRES_URL, cls.key)
        cls.store.initialize()

    def setUp(self):
        self.name = "test-" + uuid.uuid4().hex
        self.store.upsert(
            self.name,
            "loki",
            {"url": "https://loki.example.com", "scope_value": "checkout"},
            {"token": "plain-secret"},
            service="checkout",
        )

    def tearDown(self):
        self.store.delete(self.name)

    def test_factory_and_encrypted_source_round_trip(self):
        selected = create_log_source_store("unused.db", self.key, POSTGRES_URL)
        self.assertIsInstance(selected, PostgresLogSourceStore)
        saved = selected.get(self.name)
        self.assertEqual(saved["service"], "checkout")
        self.assertEqual(saved["credentials"], {"token": "plain-secret"})

        with selected._connect() as connection:
            row = connection.execute(
                "SELECT secret_nonce, secret_ciphertext FROM log_sources WHERE name = %s",
                (self.name,),
            ).fetchone()
        self.assertNotIn(b"plain-secret", bytes(row["secret_nonce"]) + bytes(row["secret_ciphertext"]))

    def test_replica_safe_watcher_claim_and_checkpoint(self):
        self.store.upsert_watcher(
            self.name,
            enabled=True,
            interval_s=30,
            lag_s=5,
            initial_lookback_s=60,
        )
        replica = PostgresLogSourceStore(POSTGRES_URL, self.key)
        first = self.store.claim_due_watchers(1000, "replica-a")
        second = replica.claim_due_watchers(1000, "replica-b")
        self.assertEqual([item["source_name"] for item in first], [self.name])
        self.assertEqual(second, [])

        self.store.watcher_succeeded(
            self.name,
            checkpoint=995,
            completed=1000,
            next_run=1030,
            records=4,
            requests=2,
            bytes_received=500,
            owner="replica-a",
        )
        watcher = replica.get_watcher(self.name)
        self.assertEqual(watcher["checkpoint_unix_s"], 995)
        self.assertEqual(watcher["last_records"], 4)

    def test_detection_history_is_shared_and_removed_with_source(self):
        self.store.add_detection(
            {
                "source": self.name,
                "at_unix_ms": 1000,
                "kind": "watcher_run",
                "reasons": ["error_pattern"],
            }
        )
        replica = PostgresLogSourceStore(POSTGRES_URL, self.key)
        self.assertEqual(replica.list_detections(self.name)[0]["kind"], "watcher_run")
        self.assertTrue(replica.delete(self.name))
        self.assertEqual(replica.list_detections(self.name), [])


if __name__ == "__main__":
    unittest.main()
