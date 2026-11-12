from __future__ import annotations

import json
import socket
import threading
import time
import uuid
from collections.abc import Callable
from typing import Any


def _producer_factory(config: dict[str, Any]):
    try:
        from confluent_kafka import Producer
    except ImportError as exc:
        raise RuntimeError("Kafka transport requires the confluent-kafka package") from exc
    return Producer(config)


def _consumer_factory(config: dict[str, Any]):
    try:
        from confluent_kafka import Consumer
    except ImportError as exc:
        raise RuntimeError("Kafka processing requires the confluent-kafka package") from exc
    return Consumer(config)


class KafkaOutboxPublisher:
    """Publishes the durable Kubernetes database outbox to Kafka-compatible brokers."""

    def __init__(
        self,
        store,
        bootstrap_servers: str,
        topic: str,
        *,
        producer_factory: Callable[[dict[str, Any]], Any] = _producer_factory,
        producer_config: dict[str, Any] | None = None,
        poll_s: float = 1.0,
    ):
        if not bootstrap_servers.strip():
            raise ValueError("AIDBG_KAFKA_BOOTSTRAP_SERVERS is required in Kafka mode")
        if not topic.strip():
            raise ValueError("AIDBG_KAFKA_K8S_TOPIC must not be empty")
        self.store = store
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.poll_s = poll_s
        self.owner = f"{socket.gethostname()}:{uuid.uuid4().hex}"
        config = {
            "bootstrap.servers": bootstrap_servers,
            "client.id": "quellops-kubernetes-outbox",
            "enable.idempotence": True,
            "acks": "all",
            "compression.type": "zstd",
            "message.timeout.ms": 10000,
        }
        config.update(producer_config or {})
        self.producer = producer_factory(config)
        self.stop_event = threading.Event()
        self.thread: threading.Thread | None = None
        self._lock = threading.Lock()
        self._last_error: str | None = None
        self._last_publish_at: float | None = None

    def start(self) -> None:
        if self.thread and self.thread.is_alive():
            return
        self.stop_event.clear()
        self.thread = threading.Thread(
            target=self._loop,
            name="quellops-kubernetes-kafka-outbox",
            daemon=True,
        )
        self.thread.start()

    def stop(self) -> None:
        self.stop_event.set()
        if self.thread:
            self.thread.join(timeout=10)
        self.producer.flush(5)

    def _set_result(self, error: str | None, published: bool = False) -> None:
        with self._lock:
            self._last_error = error
            if published:
                self._last_publish_at = time.time()

    def run_once(self) -> int:
        rows = self.store.claim_transport_batch(self.owner, limit=100, lease_s=30)
        if not rows:
            return 0
        delivered: set[int] = set()
        failures: dict[int, str] = {}

        def callback(row_id: int):
            def complete(error, _message) -> None:
                if error is None:
                    delivered.add(row_id)
                else:
                    failures[row_id] = str(error)

            return complete

        try:
            for row in rows:
                envelope = {
                    "schema_version": 1,
                    "outbox_id": row["id"],
                    "cluster_id": row["cluster_id"],
                    "event_id": row["event_id"],
                    "event": row["event"],
                }
                self.producer.produce(
                    self.topic,
                    # Keep one cluster's event sequence on the same partition so
                    # stateful correlation sees it in order.
                    key=row["cluster_id"].encode(),
                    value=json.dumps(envelope, sort_keys=True, separators=(",", ":")).encode(),
                    on_delivery=callback(row["id"]),
                )
                self.producer.poll(0)
            remaining = self.producer.flush(10)
            if remaining:
                if hasattr(self.producer, "purge"):
                    self.producer.purge()
                    self.producer.flush(1)
                for row in rows:
                    if row["id"] not in delivered and row["id"] not in failures:
                        failures[row["id"]] = "Kafka delivery timed out"
        except Exception as exc:
            message = str(exc)[:1000]
            pending = [row["id"] for row in rows if row["id"] not in delivered]
            self.store.mark_transport_complete(self.owner, sorted(delivered))
            self.store.release_transport(self.owner, pending, message)
            self._set_result(message)
            return len(delivered)

        successful = sorted(delivered - failures.keys())
        self.store.mark_transport_complete(self.owner, successful)
        for row_id, error in failures.items():
            self.store.release_transport(self.owner, [row_id], error)
        error = next(iter(failures.values()), None)
        self._set_result(error, published=bool(successful))
        return len(successful)

    def _loop(self) -> None:
        while not self.stop_event.is_set():
            try:
                published = self.run_once()
            except Exception as exc:
                self._set_result(str(exc)[:1000])
                published = 0
            self.stop_event.wait(0.05 if published else self.poll_s)

    def status(self) -> dict[str, Any]:
        with self._lock:
            error = self._last_error
            published_at = self._last_publish_at
        return {
            "mode": "kafka",
            "healthy": error is None,
            "topic": self.topic,
            "backlog": self.store.count_transport_backlog(),
            "last_error": error,
            "last_publish_at": published_at,
        }


class KafkaEventConsumer:
    """Dedicated at-least-once worker for normalized Kubernetes events."""

    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
        group_id: str,
        handler: Callable[[dict[str, Any]], None],
        *,
        consumer_factory: Callable[[dict[str, Any]], Any] = _consumer_factory,
        consumer_config: dict[str, Any] | None = None,
        poll_s: float = 1.0,
    ):
        if not bootstrap_servers.strip():
            raise ValueError("Kafka bootstrap servers are required")
        if not topic.strip() or not group_id.strip():
            raise ValueError("Kafka topic and consumer group are required")
        config = {
            "bootstrap.servers": bootstrap_servers,
            "group.id": group_id,
            "client.id": f"quellops-central-consumer-{socket.gethostname()}",
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
            "auto.offset.reset": "earliest",
            "isolation.level": "read_committed",
        }
        config.update(consumer_config or {})
        self.topic = topic
        self.group_id = group_id
        self.handler = handler
        self.poll_s = poll_s
        self.consumer = consumer_factory(config)
        self.stop_event = threading.Event()
        self.thread: threading.Thread | None = None
        self._lock = threading.Lock()
        self._last_error: str | None = None
        self._last_processed_at: float | None = None
        self._last_partition: int | None = None
        self._last_offset: int | None = None
        self._lag: int | None = None
        self._processed = 0
        self._failures = 0

    def start(self) -> None:
        if self.thread and self.thread.is_alive():
            return
        self.consumer.subscribe([self.topic])
        self.stop_event.clear()
        self.thread = threading.Thread(
            target=self._loop,
            name="quellops-kubernetes-kafka-consumer",
            daemon=True,
        )
        self.thread.start()

    def stop(self) -> None:
        self.stop_event.set()
        if self.thread:
            self.thread.join(timeout=10)
        self.consumer.close()

    def _set_failure(self, error: str) -> None:
        with self._lock:
            self._last_error = error[:1000]
            self._failures += 1

    def run_once(self, timeout: float | None = None) -> bool:
        message = self.consumer.poll(self.poll_s if timeout is None else timeout)
        if message is None:
            return False
        error = message.error()
        if error is not None:
            self._set_failure(str(error))
            return False
        try:
            envelope = json.loads(message.value())
            if int(envelope.get("schema_version") or 0) != 1:
                raise ValueError("unsupported Kafka event schema")
            event = dict(envelope.get("event") or {})
            event["cluster_id"] = str(envelope.get("cluster_id") or "")
            event["event_id"] = str(envelope.get("event_id") or event.get("event_id") or "")
            if not event["cluster_id"] or not event["event_id"]:
                raise ValueError("Kafka event is missing cluster_id or event_id")
            self.handler(event)
            self.consumer.store_offsets(message=message)
            self.consumer.commit(message=message, asynchronous=False)
            lag = None
            try:
                from confluent_kafka import TopicPartition

                _low, high = self.consumer.get_watermark_offsets(
                    TopicPartition(message.topic(), message.partition()), cached=False
                )
                lag = max(0, int(high) - int(message.offset()) - 1)
            except Exception:
                pass
            with self._lock:
                self._last_error = None
                self._last_processed_at = time.time()
                self._last_partition = int(message.partition())
                self._last_offset = int(message.offset())
                self._lag = lag
                self._processed += 1
            return True
        except Exception as exc:
            # Rewind this partition. Committing a later offset must never skip a
            # message whose incident processing failed.
            try:
                from confluent_kafka import TopicPartition

                self.consumer.seek(
                    TopicPartition(message.topic(), message.partition(), message.offset())
                )
            except Exception:
                pass
            self._set_failure(str(exc))
            return False

    def _loop(self) -> None:
        while not self.stop_event.is_set():
            if not self.run_once():
                self.stop_event.wait(0.25)

    def status(self) -> dict[str, Any]:
        with self._lock:
            return {
                "group": self.group_id,
                "healthy": self._last_error is None,
                "last_error": self._last_error,
                "last_processed_at": self._last_processed_at,
                "partition": self._last_partition,
                "offset": self._last_offset,
                "lag": self._lag,
                "processed": self._processed,
                "failures": self._failures,
            }
