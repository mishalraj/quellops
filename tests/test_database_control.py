import base64
import tempfile
import unittest

from aidbg.database_control import router
from aidbg.engine import SignalStore
from fastapi import FastAPI
from fastapi.testclient import TestClient


class DatabaseControlTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.signals = SignalStore()
        app = FastAPI()
        app.include_router(
            router(
                "admin-secret",
                base64.b64encode(b"k" * 32).decode(),
                self.temp.name + "/control.db",
                self.signals,
            )
        )
        self.client_context = TestClient(app)
        self.client = self.client_context.__enter__()
        self.headers = {"Authorization": "Bearer admin-secret"}

    def tearDown(self):
        self.client_context.__exit__(None, None, None)
        self.temp.cleanup()

    def test_admin_auth_and_end_to_end_cloud_collection(self):
        self.assertEqual(self.client.get("/admin/databases/catalog").status_code, 401)
        payload = {
            "name": "orders",
            "provider": "aws",
            "engine": "rds-postgresql",
            "access_mode": "cloud_metrics",
            "service": "orders",
            "config": {
                "resource_id": "orders-replica",
                "test_points": [
                    {"metric": "ReplicaLag", "value": 7, "unit": "seconds", "severity": "warning"}
                ],
            },
            "credentials": {"secret_access_key": "secret"},
        }
        response = self.client.put(
            "/admin/databases/sources/orders", headers=self.headers, json=payload
        )
        self.assertEqual(response.status_code, 200, response.text)
        public = self.client.get("/admin/databases/sources", headers=self.headers).json()
        self.assertNotIn("credentials", public["sources"][0])
        self.client.put(
            "/admin/databases/sources/orders/watcher",
            headers=self.headers,
            json={"enabled": True, "interval_s": 60},
        )
        run = self.client.post("/admin/databases/sources/orders/run", headers=self.headers)
        self.assertEqual(run.status_code, 200, run.text)
        self.assertEqual(run.json()["signals"], 1)
        stored = self.client.get("/admin/databases/signals", headers=self.headers).json()
        self.assertEqual(stored["signals"][0]["signal"], "replica_lag")
        self.assertTrue(
            any(item["kind"] == "db.replica_lag" for item in self.signals.recent_change_events())
        )

    def test_direct_primary_opt_in_is_rejected(self):
        payload = {
            "name": "unsafe",
            "provider": "self_hosted",
            "engine": "postgresql",
            "access_mode": "replica",
            "config": {"host": "primary", "allow_primary": True},
        }
        response = self.client.put(
            "/admin/databases/sources/unsafe", headers=self.headers, json=payload
        )
        self.assertEqual(response.status_code, 400)
        self.assertIn("primary", response.json()["detail"])

    def test_resource_discovery_and_history(self):
        discovered = self.client.post(
            "/admin/databases/discover",
            headers=self.headers,
            json={
                "provider": "aws",
                "engine": "rds-postgresql",
                "config": {"test_resources": [{"id": "orders-ro", "name": "Orders"}]},
                "credentials": {},
            },
        )
        self.assertEqual(discovered.status_code, 200, discovered.text)
        self.assertEqual(discovered.json()["resources"][0]["id"], "orders-ro")
        history = self.client.get("/admin/databases/history", headers=self.headers)
        self.assertEqual(history.status_code, 200)
        self.assertIn("series", history.json())

    def test_verified_credential_rotation_and_database_incident_evidence(self):
        payload = {
            "name": "orders",
            "provider": "aws",
            "engine": "rds-postgresql",
            "access_mode": "cloud_metrics",
            "service": "checkout-service",
            "config": {
                "resource_id": "orders-ro",
                "test_points": [
                    {"metric": "ReplicaLag", "value": 90, "unit": "seconds", "severity": "critical"}
                ],
            },
            "credentials": {"token": "old"},
        }
        self.client.put("/admin/databases/sources/orders", headers=self.headers, json=payload)
        rotated = self.client.post(
            "/admin/databases/sources/orders/credentials/rotate",
            headers=self.headers,
            json={"credentials": {"token": "new"}},
        )
        self.assertEqual(rotated.status_code, 200, rotated.text)
        self.assertEqual(rotated.json()["source"]["credential_version"], 2)
        self.client.put(
            "/admin/databases/sources/orders/watcher",
            headers=self.headers,
            json={"enabled": True, "interval_s": 60},
        )
        self.client.post("/admin/databases/sources/orders/run", headers=self.headers)
        incident = self.signals.build_incident(300, 900)
        self.assertEqual(incident["database_evidence"][0]["service"], "checkout-service")
        self.assertTrue(any(cause["type"] == "db_replica_lag" for cause in incident["top_causes"]))

    def test_empty_rotation_cannot_replace_active_credentials(self):
        payload = {
            "name": "orders",
            "provider": "aws",
            "engine": "rds-postgresql",
            "access_mode": "cloud_metrics",
            "config": {"resource_id": "orders-ro", "test_points": []},
            "credentials": {"token": "old"},
        }
        self.client.put("/admin/databases/sources/orders", headers=self.headers, json=payload)
        response = self.client.post(
            "/admin/databases/sources/orders/credentials/rotate",
            headers=self.headers,
            json={"credentials": {}},
        )
        self.assertEqual(response.status_code, 400)

    def test_collection_failure_creates_connection_health_evidence(self):
        payload = {
            "name": "broken",
            "provider": "aws",
            "engine": "rds-postgresql",
            "access_mode": "cloud_metrics",
            "service": "checkout-service",
            "config": {
                "resource_id": "broken-ro",
                "test_points": [{"metric": "CPUUtilization", "value": "invalid"}],
            },
        }
        self.client.put("/admin/databases/sources/broken", headers=self.headers, json=payload)
        self.client.put(
            "/admin/databases/sources/broken/watcher",
            headers=self.headers,
            json={"enabled": True, "interval_s": 60},
        )
        response = self.client.post("/admin/databases/sources/broken/run", headers=self.headers)
        self.assertEqual(response.status_code, 400)
        evidence = self.signals.recent_database_signals()
        self.assertEqual(evidence[-1]["signal"], "connection_health")
        self.assertEqual(evidence[-1]["severity"], "warning")
        self.assertFalse(evidence[-1]["evidence"]["healthy"])


if __name__ == "__main__":
    unittest.main()
