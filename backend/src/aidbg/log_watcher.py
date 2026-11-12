from __future__ import annotations

import random
import threading
import time
import uuid
from collections.abc import Callable

from .log_client import LogClient, LogClientError
from .log_store import LogSourceStore


class LogWatcher:
    """Runs complete bounded fetches and advances durable checkpoints."""

    def __init__(
        self,
        store: LogSourceStore,
        client: LogClient,
        observer: Callable[[dict], None] | None = None,
        random_source: random.Random | None = None,
    ):
        self.store = store
        self.client = client
        self.observer = observer
        self.owner = uuid.uuid4().hex
        self._random = random_source or random.Random()
        self._stop = threading.Event()
        self._run_lock = threading.Lock()
        self._thread: threading.Thread | None = None

    def _emit(self, batch: dict) -> None:
        if self.observer is None:
            return
        try:
            self.observer(batch)
        except Exception:
            # Analysis must never block checkpoint progress.
            pass

    def start(self) -> None:
        if self._thread is not None:
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._loop, name="aidbg-log-watcher", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=5)
            self._thread = None

    def _loop(self) -> None:
        while not self._stop.is_set():
            now = time.time()
            try:
                due = self.store.claim_due_watchers(now, self.owner)
            except Exception:
                self._stop.wait(1)
                continue
            for watcher in due:
                if self._stop.is_set():
                    break
                try:
                    self._run_once(watcher["source_name"], now=time.time(), claimed=True)
                except Exception as exc:
                    try:
                        failures = int(watcher.get("consecutive_failures") or 0) + 1
                        delay = min(watcher["interval_s"] * (2 ** (failures - 1)), 3600)
                        self.store.watcher_failed(
                            watcher["source_name"],
                            error=str(exc),
                            next_run=time.time() + delay,
                            owner=self.owner,
                        )
                    except Exception:
                        pass
            self._stop.wait(1)

    def run_once(self, name: str, *, now: float | None = None) -> dict:
        with self._run_lock:
            now = time.time() if now is None else now
            if not self.store.claim_watcher(name, now, self.owner):
                return {"ok": False, "source": name, "error": "watcher is already running"}
            return self._run_once(name, now=now, claimed=True)

    def _run_once(self, name: str, *, now: float | None = None, claimed: bool = False) -> dict:
        now = time.time() if now is None else now
        watcher = self.store.get_watcher(name)
        source = self.store.get(name)
        if watcher is None or source is None:
            raise ValueError("log watcher source is not configured")
        interval = watcher["interval_s"]
        if not claimed and not self.store.claim_watcher(name, now, self.owner):
            return {"ok": False, "source": name, "error": "watcher is already running"}
        self.store.watcher_started(name, now, now + interval)
        end = now - watcher["lag_s"]
        start = watcher["checkpoint_unix_s"]
        if start is None:
            start = end - watcher["initial_lookback_s"]
        max_scope = int(source["config"].get("max_scope_s", 900))
        chunk_end = min(end, start + max_scope)
        if chunk_end <= start:
            self.store.watcher_succeeded(
                name,
                checkpoint=start,
                completed=now,
                next_run=now + interval,
                records=0,
                requests=0,
                bytes_received=0,
                owner=self.owner,
            )
            return {"ok": True, "source": name, "records": 0, "checkpoint": start}
        try:
            result = self.client.fetch_complete(name, start, chunk_end)
            if not result.get("coverage_complete"):
                raise LogClientError("log service did not confirm complete coverage")
            usage = result.get("usage", {})
            fetched_records = list(result.get("records", []))
            records = len(fetched_records)
            request_cost = max(
                0.0,
                float(source["config"].get("estimated_cost_per_1000_requests_usd") or 0),
            )
            cost_usd = int(usage.get("backend_requests", 0)) * request_cost / 1000.0
            caught_up = chunk_end >= end
            self.store.watcher_succeeded(
                name,
                checkpoint=chunk_end,
                completed=now,
                next_run=(
                    now + interval + self._random.uniform(0, max(1.0, interval * 0.1))
                    if caught_up
                    else now + 1
                ),
                records=records,
                requests=int(usage.get("backend_requests", 0)),
                bytes_received=int(usage.get("bytes_received", 0)),
                cost_usd=cost_usd,
                owner=self.owner,
            )
            self._emit(
                {
                    "ok": True,
                    "source": name,
                    "provider": source["provider"],
                    "service": source.get("service", ""),
                    "window_start_unix_s": start,
                    "window_end_unix_s": chunk_end,
                    "records": fetched_records,
                    "previous_records": watcher["last_records"],
                    "requests": int(usage.get("backend_requests", 0)),
                    "bytes_received": int(usage.get("bytes_received", 0)),
                    "caught_up": caught_up,
                    "max_scope_s": max_scope,
                }
            )
            return {
                "ok": True,
                "source": name,
                "records": records,
                "checkpoint": chunk_end,
                "caught_up": caught_up,
            }
        except Exception as exc:
            failures = int(watcher.get("consecutive_failures") or 0) + 1
            delay = min(interval * (2 ** (failures - 1)), 3600)
            delay += self._random.uniform(0, max(1.0, delay * 0.1))
            self.store.watcher_failed(
                name,
                error=str(exc),
                next_run=now + delay,
                owner=self.owner,
            )
            self._emit(
                {
                    "ok": False,
                    "source": name,
                    "provider": source["provider"],
                    "service": source.get("service", ""),
                    "window_start_unix_s": start,
                    "window_end_unix_s": chunk_end,
                    "previous_records": watcher["last_records"],
                    "error": str(exc),
                }
            )
            return {"ok": False, "source": name, "error": str(exc)}
