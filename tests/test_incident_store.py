from __future__ import annotations

import os
import tempfile
import time
import unittest
import uuid
from pathlib import Path

from aidbg.api import create_app
from aidbg.config import Settings
from aidbg.engine import K8sSignal
from aidbg.incident_store import (
    IncidentStore,
    PostgresIncidentStore,
    is_meaningful_incident,
)
from aidbg.ingest_otlp import StatsStore
from fastapi.testclient import TestClient


class IncidentStoreTests(unittest.TestCase):
    def test_only_actionable_snapshots_are_meaningful(self):
        self.assertFalse(
            is_meaningful_incident(
                {
                    "anomalies": [],
                    "top_causes": [],
                    "log_evidence": [],
                }
            )
        )
        self.assertTrue(
            is_meaningful_incident(
                {
                    "anomalies": [],
                    "top_causes": [{"type": "dependency_unavailable"}],
                    "log_evidence": [],
                }
            )
        )

    def test_snapshots_survive_restart_and_upsert_by_trigger(self):
        with tempfile.TemporaryDirectory() as directory:
            path = str(Path(directory) / "control.db")
            store = IncidentStore(path, retention=10)
            store.initialize()
            incident = {"generated_at_unix_ms": 1000, "top_causes": [{"type": "x"}]}
            store.save("k8s:c:e", incident, trigger_type="kubernetes_event", trigger_id="e")
            incident["generated_at_unix_ms"] = 2000
            store.save("k8s:c:e", incident, trigger_type="kubernetes_event", trigger_id="e")
            reopened = IncidentStore(path)
            reopened.initialize()
            self.assertEqual(len(reopened.list()), 1)
            self.assertEqual(reopened.latest()["generated_at_unix_ms"], 2000)

    def test_consumer_worker_status_is_shared_and_detects_stale_heartbeat(self):
        with tempfile.TemporaryDirectory() as directory:
            path = str(Path(directory) / "control.db")
            writer = IncidentStore(path)
            writer.initialize()
            writer.save_worker_status(
                "quellops-central-v1",
                {"group": "quellops-central-v1", "healthy": True, "lag": 3},
            )
            reader = IncidentStore(path)
            current = reader.worker_status("quellops-central-v1")
            self.assertTrue(current["healthy"])
            self.assertEqual(current["lag"], 3)
            stale = reader.worker_status("quellops-central-v1", stale_after_s=-1)
            self.assertFalse(stale["healthy"])
            self.assertIn("stale", stale["last_error"])

    def test_latest_incident_api_falls_back_to_durable_result_after_restart(self):
        with tempfile.TemporaryDirectory() as directory:
            path = str(Path(directory) / "control.db")
            settings = Settings(
                control_db_path=path,
                log_admin_token="admin",
                k8s_agent_token="",
            )
            first_stats = StatsStore()
            first_stats.signals.add_k8s_event(
                K8sSignal(
                    type="k8s.pod.failed",
                    at_unix_ms=int(time.time() * 1000),
                    cluster="test",
                    namespace="production",
                    name="checkout",
                    kind="Pod",
                    reason="Unhealthy",
                    message="CrashLoopBackOff: back-off restarting failed container",
                    labels={"app": "checkout"},
                    fields={},
                )
            )
            with TestClient(create_app(first_stats, settings)) as client:
                self.assertEqual(client.get("/incident/latest").status_code, 401)
                first = client.get(
                    "/incident/latest",
                    headers={"Authorization": "Bearer admin"},
                ).json()
                self.assertTrue(first["top_causes"])
                self.assertEqual(client.get("/admin/incidents/history").status_code, 401)
            with TestClient(create_app(StatsStore(), settings)) as client:
                restored = client.get(
                    "/incident/latest",
                    headers={"Authorization": "Bearer admin"},
                ).json()
                self.assertTrue(restored["top_causes"])
                self.assertEqual(restored["persistence"]["trigger_type"], "api_snapshot")
                history = client.get(
                    "/admin/incidents/history",
                    headers={"Authorization": "Bearer admin"},
                )
                self.assertEqual(history.status_code, 200)
                self.assertGreaterEqual(len(history.json()["incidents"]), 1)


@unittest.skipUnless(
    os.getenv("AIDBG_TEST_POSTGRES_URL"),
    "AIDBG_TEST_POSTGRES_URL is not configured",
)
class PostgresIncidentStoreTests(unittest.TestCase):
    def test_shared_persistent_incident_result(self):
        key = "test:" + uuid.uuid4().hex
        store = PostgresIncidentStore(os.environ["AIDBG_TEST_POSTGRES_URL"])
        store.initialize()
        store.save(
            key,
            {"generated_at_unix_ms": 1000, "top_causes": []},
            trigger_type="test",
            trigger_id=key,
        )
        replica = PostgresIncidentStore(os.environ["AIDBG_TEST_POSTGRES_URL"])
        replica.initialize()
        self.assertEqual(replica.latest()["persistence"]["incident_key"], key)
        self.assertEqual(replica.list(1)[0]["persistence"]["incident_key"], key)
        replica.save_worker_status("test-worker", {"group": "test-worker", "healthy": True})
        self.assertTrue(replica.worker_status("test-worker")["healthy"])
        with store._connect() as connection:
            connection.execute("DELETE FROM incident_results WHERE incident_key=%s", (key,))
            connection.execute(
                "DELETE FROM consumer_worker_status WHERE group_id=%s", ("test-worker",)
            )


if __name__ == "__main__":
    unittest.main()
