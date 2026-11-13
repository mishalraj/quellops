#!/usr/bin/env python3
"""Stateful local control-plane receiver used by the Kubernetes soak test."""

from __future__ import annotations

import argparse
import json
import os
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path


class State:
    def __init__(self, path: Path, namespace: str, bootstrap_token: str) -> None:
        self.path = path
        self.namespace = namespace
        self.bootstrap_token = bootstrap_token
        self.cluster_token = "soak-cluster-token"
        self.lock = threading.Lock()
        self.data = {
            "event_ids": [],
            "event_types": {},
            "duplicates": 0,
            "registrations": 0,
            "heartbeats": 0,
            "last_queue_depth": 0,
            "last_dropped_events": 0,
        }
        if path.exists():
            self.data.update(json.loads(path.read_text()))

    def save(self) -> None:
        temporary = self.path.with_suffix(".tmp")
        temporary.write_text(json.dumps(self.data, sort_keys=True))
        os.replace(temporary, self.path)

    def config(self, cluster_id: str, issue_token: bool = False) -> dict:
        result = {
            "cluster_id": cluster_id,
            "config_revision": 1,
            "namespaces": [self.namespace],
            "poll_interval_s": 2,
            "max_batch_events": 100,
        }
        if issue_token:
            result["agent_token"] = self.cluster_token
        return result


class Handler(BaseHTTPRequestHandler):
    server_version = "QuellOpsSoakReceiver/1.0"

    @property
    def state(self) -> State:
        return self.server.state  # type: ignore[attr-defined]

    def log_message(self, format: str, *args: object) -> None:
        return

    def response(self, status: int, payload: object) -> None:
        encoded = json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def authorized(self) -> bool:
        token = self.headers.get("Authorization", "").removeprefix("Bearer ")
        return token in {self.state.bootstrap_token, self.state.cluster_token}

    def body(self) -> dict:
        length = int(self.headers.get("Content-Length", "0"))
        return json.loads(self.rfile.read(length) or b"{}")

    def do_GET(self) -> None:
        if self.path == "/healthz":
            self.response(200, {"ok": True})
            return
        if self.path == "/stats":
            with self.state.lock:
                data = dict(self.state.data)
                data["accepted"] = len(data.pop("event_ids"))
            self.response(200, data)
            return
        self.response(404, {"error": "not found"})

    def do_POST(self) -> None:
        if not self.authorized():
            self.response(401, {"error": "invalid agent token"})
            return
        payload = self.body()
        cluster_id = str(payload.get("cluster_id") or "soak-cluster")
        if self.path == "/api/kubernetes/v1/register":
            with self.state.lock:
                self.state.data["registrations"] += 1
                self.state.save()
            self.response(200, {"config": self.state.config(cluster_id, issue_token=True)})
            return
        if self.path == "/api/kubernetes/v1/heartbeat":
            with self.state.lock:
                self.state.data["heartbeats"] += 1
                self.state.data["last_queue_depth"] = int(payload.get("queue_depth") or 0)
                self.state.data["last_dropped_events"] = int(payload.get("dropped_events") or 0)
                self.state.save()
            self.response(200, {"config": self.state.config(cluster_id)})
            return
        if self.path == "/api/kubernetes/v1/events":
            accepted = 0
            duplicates = 0
            with self.state.lock:
                known = set(self.state.data["event_ids"])
                for event in payload.get("events") or []:
                    event_id = str(event.get("event_id") or "")
                    if not event_id or event_id in known:
                        duplicates += 1
                        continue
                    known.add(event_id)
                    self.state.data["event_ids"].append(event_id)
                    event_type = str(event.get("type") or "unknown")
                    types = self.state.data["event_types"]
                    types[event_type] = int(types.get(event_type, 0)) + 1
                    accepted += 1
                self.state.data["duplicates"] += duplicates
                self.state.save()
            self.response(200, {"accepted": accepted, "duplicates": duplicates})
            return
        self.response(404, {"error": "not found"})


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--state", type=Path, required=True)
    parser.add_argument("--namespace", default="quellops-soak")
    parser.add_argument("--bootstrap-token", default="soak-bootstrap-token")
    args = parser.parse_args()
    args.state.parent.mkdir(parents=True, exist_ok=True)
    server = ThreadingHTTPServer(("0.0.0.0", args.port), Handler)
    server.state = State(args.state, args.namespace, args.bootstrap_token)  # type: ignore[attr-defined]
    server.serve_forever()


if __name__ == "__main__":
    main()
