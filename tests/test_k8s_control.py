from __future__ import annotations

import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from aidbg.engine import SignalStore
from aidbg.k8s_control import router
from aidbg.k8s_store import KubernetesStore
from fastapi import FastAPI
from fastapi.testclient import TestClient

AGENT_HEADERS = {"Authorization": "Bearer agent-secret"}
ADMIN_HEADERS = {"Authorization": "Bearer admin-secret"}


class KubernetesControlTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.db = str(Path(self.temp.name) / "control.db")
        self.signals = SignalStore()
        self.app = FastAPI()
        self.app.include_router(router("agent-secret", "admin-secret", self.db, self.signals))

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
            "workloads": [{"namespace": "production", "name": "checkout", "kind": "Deployment"}],
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
            cluster_headers = {
                "Authorization": "Bearer " + response.json()["config"]["agent_token"]
            }

            # Default-deny: discovered namespaces do not become monitored automatically.
            response = client.post(
                "/api/kubernetes/v1/events",
                json={"cluster_id": "k8s:cluster-123", "events": [self.event()]},
                headers=cluster_headers,
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
            response = client.post("/api/kubernetes/v1/events", json=batch, headers=cluster_headers)
            self.assertEqual(response.json()["accepted"], 1)
            response = client.post("/api/kubernetes/v1/events", json=batch, headers=cluster_headers)
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
            self.assertEqual(status.json()["transport"]["mode"], "database")
            self.assertTrue(status.json()["transport"]["healthy"])
            events = client.get("/admin/kubernetes/events", headers=ADMIN_HEADERS)
            self.assertEqual(len(events.json()["events"]), 1)

        # A second backend replica can read the same durable cluster and event state.
        replica_store = KubernetesStore(self.db)
        replica_store.initialize()
        self.assertEqual(
            replica_store.get("k8s:cluster-123")["selected_namespaces"], ["production"]
        )
        self.assertEqual(len(replica_store.list_events()), 1)

    def test_delete_removes_cluster_events(self):
        with TestClient(self.app) as client:
            registration = client.post(
                "/api/kubernetes/v1/register", json=self.registration(), headers=AGENT_HEADERS
            )
            cluster_headers = {
                "Authorization": "Bearer " + registration.json()["config"]["agent_token"]
            }
            client.put(
                "/admin/kubernetes/clusters/k8s:cluster-123/scope",
                json={"namespaces": ["production"]},
                headers=ADMIN_HEADERS,
            )
            client.post(
                "/api/kubernetes/v1/events",
                json={"cluster_id": "k8s:cluster-123", "events": [self.event()]},
                headers=cluster_headers,
            )
            response = client.delete(
                "/admin/kubernetes/clusters/k8s:cluster-123", headers=ADMIN_HEADERS
            )
            self.assertEqual(response.status_code, 200)
            self.assertEqual(
                client.get("/admin/kubernetes/events", headers=ADMIN_HEADERS).json()["events"],
                [],
            )

    def test_each_cluster_gets_an_individual_rotatable_credential(self):
        with TestClient(self.app) as client:
            registration = client.post(
                "/api/kubernetes/v1/register", json=self.registration(), headers=AGENT_HEADERS
            )
            token = registration.json()["config"]["agent_token"]
            individual = {"Authorization": "Bearer " + token}
            self.assertEqual(
                client.post(
                    "/api/kubernetes/v1/heartbeat",
                    json={"cluster_id": "k8s:cluster-123"},
                    headers=individual,
                ).status_code,
                200,
            )
            self.assertEqual(
                client.post(
                    "/api/kubernetes/v1/heartbeat",
                    json={"cluster_id": "k8s:cluster-123"},
                    headers=AGENT_HEADERS,
                ).status_code,
                401,
            )
            rotated = client.post(
                "/admin/kubernetes/clusters/k8s:cluster-123/rotate-token",
                json={"grace_s": 60},
                headers=ADMIN_HEADERS,
            )
            self.assertEqual(rotated.status_code, 200, rotated.text)
            new_headers = {"Authorization": "Bearer " + rotated.json()["agent_token"]}
            self.assertEqual(
                client.post(
                    "/api/kubernetes/v1/heartbeat",
                    json={"cluster_id": "k8s:cluster-123"},
                    headers=new_headers,
                ).status_code,
                200,
            )
            self.assertEqual(
                client.post(
                    "/api/kubernetes/v1/heartbeat",
                    json={"cluster_id": "k8s:cluster-123"},
                    headers=individual,
                ).status_code,
                200,
            )

    def test_configurable_retention_keeps_newest_transported_events(self):
        path = str(Path(self.temp.name) / "retention.db")
        store = KubernetesStore(path, retention_events=100)
        store.initialize()
        store.register(self.registration())
        first = [self.event(f"event-{index:04d}") for index in range(110)]
        store.add_events("k8s:cluster-123", first)
        store.mark_events_local("k8s:cluster-123", [event["event_id"] for event in first])
        final = self.event("event-final")
        store.add_events("k8s:cluster-123", [final])
        store.mark_events_local("k8s:cluster-123", [final["event_id"]])
        kept = store.list_events("k8s:cluster-123", limit=1000)
        self.assertEqual(len(kept), 100)
        self.assertEqual(kept[0]["event_id"], "event-final")

    def test_external_consumer_mode_keeps_publisher_in_api(self):
        app = FastAPI()
        with (
            patch("aidbg.k8s_control.KafkaOutboxPublisher") as publisher_type,
            patch("aidbg.k8s_control.KafkaEventConsumer") as consumer_type,
        ):
            publisher_type.return_value.status.return_value = {
                "mode": "kafka",
                "healthy": True,
                "topic": "events",
                "backlog": 0,
                "last_error": None,
                "last_publish_at": None,
            }
            app.include_router(
                router(
                    "agent-secret",
                    "admin-secret",
                    str(Path(self.temp.name) / "external.db"),
                    SignalStore(),
                    event_transport="kafka",
                    kafka_bootstrap_servers="broker:9092",
                    kafka_topic="events",
                    run_kafka_consumer=False,
                )
            )
            with TestClient(app) as client:
                status = client.get("/admin/kubernetes/status", headers=ADMIN_HEADERS).json()
            publisher_type.assert_called_once()
            consumer_type.assert_not_called()
            self.assertEqual(status["transport"]["mode"], "kafka")
            self.assertFalse(status["transport"]["consumer"]["healthy"])


if __name__ == "__main__":
    unittest.main()
