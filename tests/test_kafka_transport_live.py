from __future__ import annotations

import json
import os
import tempfile
import time
import unittest
import uuid
from pathlib import Path

from aidbg.engine import SignalStore
from aidbg.incident_store import IncidentStore
from aidbg.k8s_control import router
from fastapi import FastAPI
from fastapi.testclient import TestClient

BOOTSTRAP_SERVERS = os.getenv("AIDBG_TEST_KAFKA_BOOTSTRAP_SERVERS", "")


@unittest.skipUnless(BOOTSTRAP_SERVERS, "AIDBG_TEST_KAFKA_BOOTSTRAP_SERVERS is not configured")
class LiveKafkaTransportTests(unittest.TestCase):
    def test_agent_http_batch_reaches_kafka_topic(self):
        from confluent_kafka import Consumer

        with tempfile.TemporaryDirectory() as temp_dir:
            topic = "quellops.kubernetes.events.test-" + uuid.uuid4().hex
            cluster_id = "k8s:live-" + uuid.uuid4().hex
            event_id = "event-" + uuid.uuid4().hex
            app = FastAPI()
            incident_store = IncidentStore(str(Path(temp_dir) / "control.db"))
            app.include_router(
                router(
                    "agent-secret",
                    "admin-secret",
                    str(Path(temp_dir) / "control.db"),
                    SignalStore(),
                    event_transport="kafka",
                    kafka_bootstrap_servers=BOOTSTRAP_SERVERS,
                    kafka_topic=topic,
                    incident_store=incident_store,
                )
            )
            consumer = Consumer(
                {
                    "bootstrap.servers": BOOTSTRAP_SERVERS,
                    "group.id": "quellops-live-test-" + uuid.uuid4().hex,
                    "auto.offset.reset": "earliest",
                }
            )
            consumer.subscribe([topic])
            try:
                with TestClient(app) as client:
                    agent_headers = {"Authorization": "Bearer agent-secret"}
                    admin_headers = {"Authorization": "Bearer admin-secret"}
                    registration = {
                        "cluster_id": cluster_id,
                        "name": "live-kafka-test",
                        "agent_version": "test",
                        "kubernetes_version": "test",
                        "namespaces": ["production"],
                        "workloads": [],
                    }
                    self.assertEqual(
                        (
                            registration_response := client.post(
                                "/api/kubernetes/v1/register",
                                json=registration,
                                headers=agent_headers,
                            )
                        ).status_code,
                        200,
                    )
                    cluster_headers = {
                        "Authorization": "Bearer "
                        + registration_response.json()["config"]["agent_token"]
                    }
                    self.assertEqual(
                        client.put(
                            f"/admin/kubernetes/clusters/{cluster_id}/scope",
                            json={"namespaces": ["production"]},
                            headers=admin_headers,
                        ).status_code,
                        200,
                    )
                    response = client.post(
                        "/api/kubernetes/v1/events",
                        json={
                            "cluster_id": cluster_id,
                            "events": [
                                {
                                    "event_id": event_id,
                                    "type": "k8s.pod.failed",
                                    "at_unix_ms": int(time.time() * 1000),
                                    "namespace": "production",
                                    "name": "checkout",
                                    "kind": "Pod",
                                    "reason": "CrashLoopBackOff",
                                }
                            ],
                        },
                        headers=cluster_headers,
                    )
                    self.assertEqual(response.status_code, 200, response.text)
                    self.assertEqual(response.json()["accepted"], 1)

                    message = None
                    deadline = time.time() + 15
                    while time.time() < deadline and message is None:
                        candidate = consumer.poll(1)
                        if candidate is not None and candidate.error() is None:
                            message = candidate
                    self.assertIsNotNone(message, "Kafka event was not received")
                    envelope = json.loads(message.value())
                    self.assertEqual(envelope["cluster_id"], cluster_id)
                    self.assertEqual(envelope["event_id"], event_id)

                    status = client.get("/admin/kubernetes/status", headers=admin_headers).json()
                    self.assertEqual(status["transport"]["mode"], "kafka")
                    self.assertEqual(status["transport"]["backlog"], 0)
                    deadline = time.time() + 10
                    while (
                        time.time() < deadline
                        and status["transport"].get("consumer", {}).get("processed", 0) < 1
                    ):
                        time.sleep(0.1)
                        status = client.get(
                            "/admin/kubernetes/status", headers=admin_headers
                        ).json()
                    self.assertGreaterEqual(status["transport"]["consumer"]["processed"], 1, status)
                    self.assertTrue(status["transport"]["healthy"], status)
                    self.assertTrue(status["transport"]["consumer"]["healthy"])
                    incidents = incident_store.list()
                    self.assertTrue(incidents)
                    self.assertEqual(
                        incidents[0]["persistence"]["trigger_type"],
                        "kubernetes_event",
                    )
            finally:
                consumer.close()


if __name__ == "__main__":
    unittest.main()
