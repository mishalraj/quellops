"""Run against disposable PostgreSQL; defaults to a 180-second multi-worker soak."""

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
    duration = int(os.getenv("DATABASE_SOAK_SECONDS", "180"))
    source_count = int(os.getenv("DATABASE_SOAK_SOURCES", "12"))
    key = base64.b64encode(b"s" * 32).decode()
    prefix = "soak-" + uuid.uuid4().hex[:8]
    sources = [f"{prefix}-{index:02d}" for index in range(source_count)]
    store = create_database_store("", key, database_url)
    store.initialize()
    for index, source in enumerate(sources):
        store.upsert_source(
            source,
            "aws",
            "rds-postgresql",
            "cloud_metrics",
            f"soak-{index % 3}",
            {
                "resource_id": f"local-simulator-{index}",
                "interval_s": 15,
                "test_points": [
                    {"metric": "DatabaseConnections", "value": 12 + index, "unit": "count"}
                ],
            },
            {},
        )
        store.upsert_watcher(source, True, 15)
    workers = [
        multiprocessing.Process(target=worker, args=(database_url, key, duration)) for _ in range(4)
    ]
    for process in workers:
        process.start()
    restart_at = max(20, duration // 3)
    time.sleep(restart_at)
    workers[0].terminate()
    workers[0].join(10)
    replacement = multiprocessing.Process(
        target=worker, args=(database_url, key, duration - restart_at)
    )
    replacement.start()
    for process in workers[1:]:
        process.join(duration + 20)
    replacement.join(duration + 20)
    expected_minimum = max(3, duration // 20)
    statuses = [store.get_watcher(source) or {} for source in sources]
    saved = [store.list_signals(source, 1000) for source in sources]
    for status, signals in zip(statuses, saved, strict=True):
        assert status.get("total_runs", 0) >= expected_minimum, status
        assert len(signals) == status["total_runs"], (len(signals), status)
        assert status.get("total_failures") == 0, status
        assert status.get("last_error") is None, status
    print(
        {
            "sources": source_count,
            "duration_s": duration,
            "workers": 5,
            "worker_restart": True,
            "runs": sum(item["total_runs"] for item in statuses),
            "signals": sum(len(items) for items in saved),
            "failures": sum(item["total_failures"] for item in statuses),
        }
    )


if __name__ == "__main__":
    main()
