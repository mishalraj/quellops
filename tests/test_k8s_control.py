from __future__ import annotations

import tempfile
import time
import unittest
from pathlib import Path

from fastapi import FastAPI
from fastapi.testclient import TestClient

from aidbg.engine import SignalStore
from aidbg.k8s_control import router
from aidbg.k8s_store import KubernetesStore


AGENT_HEADERS = {"Authorization": "Bearer agent-secret"}
ADMIN_HEADERS = {"Authorization": "Bearer admin-secret"}


class KubernetesControlTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.db = str(Path(self.temp.name) / "control.db")
        self.signals = SignalStore()
        self.app = FastAPI()
        self.app.include_router(
            router("agent-secret", "admin-secret", self.db, self.signals)
        )

    def tearDown(self):
        self.temp.cleanup()

    @staticmethod
    def registration():
        return {
            "cluster_id": "k8s:cluster-123",
            "name": "production",
            "agent_version": "0.2.0",
            "kubernetes_version": "v1.34.0",
            "namespaces": ["default", "production"],
            "workloads": [
                {"namespace": "production", "name": "checkout", "kind": "Deployment"}
            ],
            "queue_depth": 0,
            "dropped_events": 0,
        }

    @staticmethod
    def event(event_id="event-0001", namespace="production"):
        return {
            "event_id": event_id,
            "type": "k8s.pod.failed",
            "at_unix_ms": int(time.time() * 1000),
            "namespace": namespace,
            "name": "checkout-123",
            "kind": "Pod",
            "reason": "CrashLoopBackOff",
            "message": "container is restarting",
            "labels": {"app": "checkout"},
            "fields": {"restarts": "4"},
        }

    def test_registration_scope_delivery_and_replica_recovery(self):
        with TestClient(self.app) as client:
            self.assertEqual(
                client.post("/api/kubernetes/v1/register", json=self.registration()).status_code,
                401,
            )
            response = client.post(
                "/api/kubernetes/v1/register", json=self.registration(), headers=AGENT_HEADERS
            )
            self.assertEqual(response.status_code, 200, response.text)
            self.assertEqual(response.json()["config"]["namespaces"], [])

            # Default-deny: discovered namespaces do not become monitored automatically.
            response = client.post(
                "/api/kubernetes/v1/events",
                json={"cluster_id": "k8s:cluster-123", "events": [self.event()]},
                headers=AGENT_HEADERS,
            )
            self.assertEqual(response.json()["rejected_scope"], 1)

            response = client.put(
                "/admin/kubernetes/clusters/k8s:cluster-123/scope",
                json={"namespaces": ["missing"]},
                headers=ADMIN_HEADERS,
            )
            self.assertEqual(response.status_code, 422)
            response = client.put(
                "/admin/kubernetes/clusters/k8s:cluster-123/scope",
                json={"namespaces": ["production"]},
                headers=ADMIN_HEADERS,
            )
            self.assertEqual(response.status_code, 200, response.text)
            self.assertEqual(response.json()["selected_namespaces"], ["production"])

            batch = {"cluster_id": "k8s:cluster-123", "events": [self.event()]}
            response = client.post(
                "/api/kubernetes/v1/events", json=batch, headers=AGENT_HEADERS
            )
            self.assertEqual(response.json()["accepted"], 1)
            response = client.post(
                "/api/kubernetes/v1/events", json=batch, headers=AGENT_HEADERS
            )
            self.assertEqual(response.json()["duplicates"], 1)

            # The bridge makes persisted events available to central correlation.
            deadline = time.time() + 4
            while time.time() < deadline and not self.signals.recent_k8s_events():
                time.sleep(0.05)
            recent = self.signals.recent_k8s_events()
            self.assertEqual(recent[-1]["cluster"], "k8s:cluster-123")
            self.assertEqual(recent[-1]["reason"], "CrashLoopBackOff")

            status = client.get("/admin/kubernetes/status", headers=ADMIN_HEADERS)
            self.assertEqual(status.json()["monitoring"], 1)
            self.assertEqual(status.json()["events"], 1)
            events = client.get("/admin/kubernetes/events", headers=ADMIN_HEADERS)
            self.assertEqual(len(events.json()["events"]), 1)

        # A second backend replica can read the same durable cluster and event state.
        replica_store = KubernetesStore(self.db)
        replica_store.initialize()
        self.assertEqual(replica_store.get("k8s:cluster-123")["selected_namespaces"], ["production"])
        self.assertEqual(len(replica_store.list_events()), 1)

    def test_delete_removes_cluster_events(self):
        with TestClient(self.app) as client:
            client.post("/api/kubernetes/v1/register", json=self.registration(), headers=AGENT_HEADERS)
            client.put(
                "/admin/kubernetes/clusters/k8s:cluster-123/scope",
                json={"namespaces": ["production"]}, headers=ADMIN_HEADERS,
            )
            client.post(
                "/api/kubernetes/v1/events",
                json={"cluster_id": "k8s:cluster-123", "events": [self.event()]},
                headers=AGENT_HEADERS,
            )
            response = client.delete(
                "/admin/kubernetes/clusters/k8s:cluster-123", headers=ADMIN_HEADERS
            )
            self.assertEqual(response.status_code, 200)
            self.assertEqual(
                client.get("/admin/kubernetes/events", headers=ADMIN_HEADERS).json()["events"],
                [],
            )


if __name__ == "__main__":
    unittest.main()
