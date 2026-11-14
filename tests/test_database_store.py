import base64
import os
import tempfile
import time
import unittest

from aidbg.database_store import DatabaseStore, create_database_store


class DatabaseStoreTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.store = DatabaseStore(
            os.path.join(self.temp.name, "control.db"),
            base64.b64encode(b"d" * 32).decode(),
        )
        self.store.initialize()

    def tearDown(self):
        self.temp.cleanup()

    def test_credentials_are_encrypted_and_not_returned_from_list(self):
        self.store.upsert_source(
            "orders",
            "aws",
            "rds-postgresql",
            "cloud_metrics",
            "orders",
            {"resource_id": "orders-replica"},
            {"secret_access_key": "very-secret"},
        )
        with self.store.path.open("rb") as stored:
            self.assertNotIn(b"very-secret", stored.read())
        self.assertNotIn("credentials", self.store.list_sources()[0])
        self.assertEqual(
            self.store.get_source("orders")["credentials"]["secret_access_key"], "very-secret"
        )

    def test_watcher_lease_stats_and_signal_retention(self):
        self.store.upsert_source(
            "orders",
            "aws",
            "rds-postgresql",
            "cloud_metrics",
            "orders",
            {"resource_id": "orders-replica"},
            {},
        )
        self.store.upsert_watcher("orders", True, 60)
        self.assertTrue(self.store.claim_one("orders", "worker-a", time.time()))
        self.assertFalse(self.store.claim_one("orders", "worker-b", time.time()))
        self.store.started("orders", "worker-a", time.time())
        self.store.succeeded("orders", "worker-a", time.time(), 2, 3, 40, 0.01)
        status = self.store.get_watcher("orders")
        self.assertEqual(status["total_runs"], 1)
        self.assertEqual(status["total_requests"], 2)
        signals = [
            {"source": "orders", "observed_at_unix_ms": index, "signal": "cpu"}
            for index in range(10)
        ]
        self.store.add_signals("orders", signals, retention=3)
        self.assertEqual(len(self.store.list_signals("orders")), 3)

    def test_factory_uses_postgres_when_url_is_set(self):
        selected = create_database_store(
            "unused", base64.b64encode(b"d" * 32).decode(), "postgresql://example/db"
        )
        self.assertEqual(selected.__class__.__name__, "PostgresDatabaseStore")

    def test_credential_rotation_reencrypts_and_versions_secret(self):
        self.store.upsert_source(
            "orders",
            "aws",
            "rds-postgresql",
            "cloud_metrics",
            "orders",
            {"resource_id": "orders-replica"},
            {"token": "old-secret"},
        )
        before = self.store.get_source("orders")
        rotated = self.store.rotate_credentials("orders", {"token": "new-secret"})
        after = self.store.get_source("orders")
        self.assertEqual(rotated["credential_version"], before["credential_version"] + 1)
        self.assertIsNotNone(rotated["credentials_rotated_at"])
        self.assertEqual(after["credentials"], {"token": "new-secret"})
        with self.store.path.open("rb") as stored:
            raw = stored.read()
        self.assertNotIn(b"old-secret", raw)
        self.assertNotIn(b"new-secret", raw)


if __name__ == "__main__":
    unittest.main()
