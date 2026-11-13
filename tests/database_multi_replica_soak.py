"""Run with DATABASE_TEST_URL against disposable PostgreSQL; defaults to a 90-second soak."""

from __future__ import annotations

import base64
import multiprocessing
import os
import time
import uuid

from aidbg.database_store import create_database_store
from aidbg.database_watcher import DatabaseWatcher


def worker(database_url: str, key: str, duration: float) -> None:
    store = create_database_store("", key, database_url)
    watcher = DatabaseWatcher(store)
    watcher.start()
    time.sleep(duration)
    watcher.stop()


def main() -> None:
    database_url = os.environ["DATABASE_TEST_URL"]
    duration = int(os.getenv("DATABASE_SOAK_SECONDS", "90"))
    key = base64.b64encode(b"s" * 32).decode()
    source = "soak-" + uuid.uuid4().hex[:10]
    store = create_database_store("", key, database_url)
    store.initialize()
    store.upsert_source(
        source, "aws", "rds-postgresql", "cloud_metrics", "soak",
        {
            "resource_id": "local-simulator", "interval_s": 15,
            "test_points": [{"metric": "DatabaseConnections", "value": 12, "unit": "count"}],
        }, {},
    )
    store.upsert_watcher(source, True, 15)
    first = multiprocessing.Process(target=worker, args=(database_url, key, duration))
    second = multiprocessing.Process(target=worker, args=(database_url, key, duration))
    first.start()
    second.start()
    restart_at = max(20, duration // 3)
    time.sleep(restart_at)
    first.terminate()
    first.join(10)
    replacement = multiprocessing.Process(target=worker, args=(database_url, key, duration - restart_at))
    replacement.start()
    second.join(duration + 20)
    replacement.join(duration + 20)
    status = store.get_watcher(source) or {}
    saved = store.list_signals(source, 1000)
    expected_minimum = max(3, duration // 20)
    assert status.get("total_runs", 0) >= expected_minimum, status
    assert len(saved) == status["total_runs"], (len(saved), status)
    assert status.get("total_failures") == 0, status
    assert status.get("last_error") is None, status
    print({
        "source": source, "duration_s": duration, "workers": 3,
        "worker_restart": True, "runs": status["total_runs"],
        "signals": len(saved), "failures": status["total_failures"],
    })


if __name__ == "__main__":
    main()
