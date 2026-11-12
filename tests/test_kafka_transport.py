from __future__ import annotations

import json
import tempfile
import time
import unittest
from pathlib import Path

from aidbg.k8s_store import KubernetesStore
from aidbg.kafka_transport import KafkaEventConsumer, KafkaOutboxPublisher


class FakeProducer:
    def __init__(self, config, error: str | None = None):
        self.config = config
        self.error = error
        self.pending = []
        self.messages = []

    def produce(self, topic, *, key, value, on_delivery):
        self.messages.append((topic, key, value))
        self.pending.append(on_delivery)

    def poll(self, _timeout):
        return 0

    def flush(self, _timeout):
        callbacks, self.pending = self.pending, []
        for callback in callbacks:
            callback(self.error, object())
        return 0


class FakeMessage:
    def __init__(self, value, *, offset=4, error=None):
        self._value = value
        self._offset = offset
        self._error = error

    def value(self):
        return self._value

    def error(self):
        return self._error

    def topic(self):
        return "quellops.kubernetes.events.v1"

    def partition(self):
        return 0

    def offset(self):
        return self._offset


class FakeConsumer:
    def __init__(self, config, messages):
        self.config = config
        self.messages = list(messages)
        self.subscriptions = []
        self.stored = []
        self.committed = []
        self.seeked = []
        self.closed = False

    def subscribe(self, topics):
        self.subscriptions = topics

    def poll(self, _timeout):
        return self.messages.pop(0) if self.messages else None

    def store_offsets(self, *, message):
        self.stored.append(message.offset())

    def commit(self, *, message, asynchronous):
        self.committed.append((message.offset(), asynchronous))

    def get_watermark_offsets(self, _partition, cached=False):
        return 0, 5

    def seek(self, partition):
        self.seeked.append(partition)

    def close(self):
        self.closed = True


class KafkaTransportTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.store = KubernetesStore(str(Path(self.temp.name) / "control.db"))
        self.store.initialize()
        self.cluster_id = "k8s:kafka-test"
        self.store.register(
            {
                "cluster_id": self.cluster_id,
                "name": "kafka-test",
                "agent_version": "test",
                "kubernetes_version": "test",
                "namespaces": ["production"],
                "workloads": [],
                "queue_depth": 0,
                "dropped_events": 0,
            }
        )
        self.event = {
            "event_id": "event-kafka-0001",
            "type": "k8s.pod.failed",
            "at_unix_ms": int(time.time() * 1000),
            "namespace": "production",
            "name": "checkout-123",
            "kind": "Pod",
            "reason": "CrashLoopBackOff",
            "message": "container is restarting",
            "labels": {"app": "checkout"},
            "fields": {"restarts": "4"},
        }
        self.store.add_events(self.cluster_id, [self.event])

    def tearDown(self):
        self.temp.cleanup()

    def test_publishes_outbox_envelope_and_marks_complete(self):
        fake = FakeProducer({})
        publisher = KafkaOutboxPublisher(
            self.store,
            "broker:9092",
            "quellops.kubernetes.events.v1",
            producer_factory=lambda _config: fake,
        )
        self.assertEqual(self.store.count_transport_backlog(), 1)
        self.assertEqual(publisher.run_once(), 1)
        self.assertEqual(self.store.count_transport_backlog(), 0)
        self.assertEqual(len(fake.messages), 1)
        topic, key, value = fake.messages[0]
        envelope = json.loads(value)
        self.assertEqual(topic, "quellops.kubernetes.events.v1")
        self.assertEqual(key, self.cluster_id.encode())
        self.assertEqual(envelope["schema_version"], 1)
        self.assertEqual(envelope["cluster_id"], self.cluster_id)
        self.assertEqual(envelope["event"]["reason"], "CrashLoopBackOff")
        self.assertTrue(publisher.status()["healthy"])

    def test_failed_delivery_returns_row_to_durable_backlog(self):
        fake = FakeProducer({}, error="broker unavailable")
        publisher = KafkaOutboxPublisher(
            self.store,
            "broker:9092",
            "quellops.kubernetes.events.v1",
            producer_factory=lambda _config: fake,
        )
        self.assertEqual(publisher.run_once(), 0)
        self.assertEqual(self.store.count_transport_backlog(), 1)
        self.assertFalse(publisher.status()["healthy"])
        self.assertIn("broker unavailable", publisher.status()["last_error"])

    def test_transport_lease_prevents_two_publishers_claiming_same_row(self):
        first = self.store.claim_transport_batch("publisher-a")
        second = self.store.claim_transport_batch("publisher-b")
        self.assertEqual(len(first), 1)
        self.assertEqual(second, [])
        self.store.release_transport("publisher-a", [first[0]["id"]], "retry")
        self.assertEqual(len(self.store.claim_transport_batch("publisher-b")), 1)

    def test_secured_broker_options_are_passed_to_kafka_client(self):
        captured = {}

        def factory(config):
            captured.update(config)
            return FakeProducer(config)

        KafkaOutboxPublisher(
            self.store,
            "broker:9092",
            "quellops.kubernetes.events.v1",
            producer_factory=factory,
            producer_config={
                "security.protocol": "SASL_SSL",
                "sasl.mechanism": "SCRAM-SHA-512",
                "sasl.username": "quellops",
                "sasl.password": "secret",
            },
        )
        self.assertEqual(captured["security.protocol"], "SASL_SSL")
        self.assertEqual(captured["sasl.mechanism"], "SCRAM-SHA-512")
        self.assertEqual(captured["sasl.username"], "quellops")
        self.assertEqual(captured["sasl.password"], "secret")
        self.assertTrue(captured["enable.idempotence"])

    def test_consumer_processes_then_commits_and_reports_progress(self):
        envelope = json.dumps(
            {
                "schema_version": 1,
                "cluster_id": self.cluster_id,
                "event_id": self.event["event_id"],
                "event": self.event,
            }
        ).encode()
        fake = FakeConsumer({}, [FakeMessage(envelope)])
        handled = []
        worker = KafkaEventConsumer(
            "broker:9092",
            "quellops.kubernetes.events.v1",
            "quellops-central-v1",
            handled.append,
            consumer_factory=lambda _config: fake,
        )
        self.assertTrue(worker.run_once(0))
        self.assertEqual(handled[0]["cluster_id"], self.cluster_id)
        self.assertEqual(fake.stored, [4])
        self.assertEqual(fake.committed, [(4, False)])
        self.assertEqual(worker.status()["processed"], 1)
        self.assertEqual(worker.status()["lag"], 0)

    def test_consumer_does_not_commit_when_processing_fails(self):
        envelope = json.dumps(
            {
                "schema_version": 1,
                "cluster_id": self.cluster_id,
                "event_id": self.event["event_id"],
                "event": self.event,
            }
        ).encode()
        fake = FakeConsumer({}, [FakeMessage(envelope)])
        worker = KafkaEventConsumer(
            "broker:9092",
            "quellops.kubernetes.events.v1",
            "quellops-central-v1",
            lambda _event: (_ for _ in ()).throw(RuntimeError("analysis failed")),
            consumer_factory=lambda _config: fake,
        )
        self.assertFalse(worker.run_once(0))
        self.assertEqual(fake.committed, [])
        self.assertEqual(worker.status()["failures"], 1)
        self.assertIn("analysis failed", worker.status()["last_error"])


if __name__ == "__main__":
    unittest.main()
