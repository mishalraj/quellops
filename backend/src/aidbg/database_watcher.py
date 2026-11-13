from __future__ import annotations

import threading
import time
import uuid
from collections.abc import Callable

from .database_connectors import connector_for
from .database_store import DatabaseStore


class DatabaseWatcher:
    """Runs bounded database collections with durable leases and backoff."""

    def __init__(
        self,
        store: DatabaseStore,
        observer: Callable[[dict], None] | None = None,
        retention: int = 5000,
    ) -> None:
        self.store = store
        self.observer = observer
        self.retention = max(100, min(retention, 100_000))
        self.owner = uuid.uuid4().hex
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        if self._thread is not None:
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._loop, name="quellops-database-watcher", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=5)
            self._thread = None

    def _loop(self) -> None:
        while not self._stop.is_set():
            try:
                names = self.store.claim_due(self.owner, time.time())
            except Exception:
                self._stop.wait(1)
                continue
            for name in names:
                if self._stop.is_set():
                    break
                self._collect_claimed(name)
            self._stop.wait(1)

    def run_once(self, name: str) -> dict:
        now = time.time()
        if not self.store.claim_one(name, self.owner, now):
            return {"ok": False, "source": name, "error": "collector is already running"}
        return self._collect_claimed(name)

    def _collect_claimed(self, name: str) -> dict:
        now = time.time()
        try:
            source = self.store.get_source(name)
            if source is None:
                raise ValueError("database source is not configured")
            self.store.started(name, self.owner, now)
            connector = connector_for(
                source["name"], source["provider"], source["engine"],
                source["access_mode"], source["config"], source["credentials"],
            )
            result = connector.collect()
            signals = [signal.as_dict() for signal in result.signals]
            self.store.add_signals(name, signals, self.retention)
            self.store.succeeded(
                name, self.owner, time.time(), result.requests, result.rows,
                result.bytes_received, result.estimated_cost_usd,
            )
            for signal in signals:
                if self.observer is not None:
                    try:
                        self.observer(signal)
                    except Exception:
                        pass
            return {
                "ok": True, "source": name, "signals": len(signals),
                "requests": result.requests, "rows": result.rows,
                "duration_ms": result.duration_ms,
                "estimated_cost_usd": result.estimated_cost_usd,
            }
        except Exception as exc:
            self.store.failed(name, self.owner, str(exc), time.time())
            return {"ok": False, "source": name, "error": str(exc)}
