from __future__ import annotations

import os
import signal
import threading

from .config import load_settings
from .engine import SignalStore
from .incident_store import create_incident_store
from .k8s_control import process_kubernetes_event
from .kafka_transport import KafkaEventConsumer


def main() -> None:
    settings = load_settings()
    if settings.event_transport.strip().lower() != "kafka":
        raise RuntimeError("AIDBG_EVENT_TRANSPORT must be kafka for the consumer worker")

    signals = SignalStore()
    signals.configure_correlation(
        db_slow_threshold_ms=settings.db_slow_query_threshold_ms,
        change_horizon_s=settings.change_horizon_s,
    )
    incidents = create_incident_store(
        settings.control_db_path,
        settings.control_database_url,
        settings.incident_retention,
    )
    incidents.initialize()
    kafka_config = {
        key: value
        for key, value in {
            "security.protocol": settings.kafka_security_protocol,
            "sasl.mechanism": settings.kafka_sasl_mechanism,
            "sasl.username": settings.kafka_sasl_username,
            "sasl.password": settings.kafka_sasl_password,
            "ssl.ca.location": settings.kafka_ssl_ca_location,
        }.items()
        if value
    }
    consumer = KafkaEventConsumer(
        settings.kafka_bootstrap_servers,
        settings.kafka_k8s_topic,
        settings.kafka_consumer_group,
        lambda event: process_kubernetes_event(
            signals,
            incidents,
            event,
            incident_builder=lambda: signals.build_incident(
                settings.current_window_s, settings.baseline_window_s
            ),
        ),
        consumer_config=kafka_config,
    )
    stop = threading.Event()

    def request_stop(_signum, _frame) -> None:
        stop.set()

    signal.signal(signal.SIGTERM, request_stop)
    signal.signal(signal.SIGINT, request_stop)
    consumer.start()
    try:
        while not stop.wait(1):
            status = consumer.status()
            status.update({"role": "standalone", "pid": os.getpid()})
            incidents.save_worker_status(settings.kafka_consumer_group, status)
    finally:
        consumer.stop()
        status = consumer.status()
        status.update(
            {
                "role": "standalone",
                "pid": os.getpid(),
                "healthy": False,
                "last_error": "consumer worker stopped",
            }
        )
        incidents.save_worker_status(settings.kafka_consumer_group, status)


if __name__ == "__main__":
    main()
