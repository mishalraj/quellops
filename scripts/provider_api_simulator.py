#!/usr/bin/env python3
"""Small Splunk and Datadog protocol simulator for connector conformance tests."""

from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs

SPLUNK_TOKEN = "simulation-token"
DATADOG_API_KEY = "simulation-api-key"
DATADOG_APP_KEY = "simulation-app-key"


class Handler(BaseHTTPRequestHandler):
    server_version = "QuellOpsProviderSimulator/1.0"

    def log_message(self, format: str, *args: object) -> None:
        return

    def send_json(self, payload: object, status: int = 200) -> None:
        encoded = json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def do_GET(self) -> None:
        if self.path == "/healthz":
            self.send_json({"ok": True})
            return
        self.send_json({"error": "not found"}, 404)

    def do_POST(self) -> None:
        length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(length)
        if self.path == "/services/search/v2/jobs/export":
            self.handle_splunk(raw)
            return
        if self.path == "/api/v2/logs/analytics/aggregate":
            self.handle_datadog_discovery(raw)
            return
        if self.path == "/api/v2/logs/events/search":
            self.handle_datadog_fetch(raw)
            return
        self.send_json({"error": "not found"}, 404)

    def datadog_authorized(self) -> bool:
        return (
            self.headers.get("DD-API-KEY") == DATADOG_API_KEY
            and self.headers.get("DD-APPLICATION-KEY") == DATADOG_APP_KEY
        )

    def handle_splunk(self, raw: bytes) -> None:
        if self.headers.get("Authorization") != f"Splunk {SPLUNK_TOKEN}":
            self.send_json({"error": "invalid Splunk token"}, 401)
            return
        search = parse_qs(raw.decode()).get("search", [""])[0]
        if "metadata type=hosts" in search:
            payload = {"preview": False, "result": {"host": "simulated-api-1"}}
        elif "metadata type=sourcetypes" in search:
            payload = {"preview": False, "result": {"sourcetype": "kube:container"}}
        else:
            payload = {
                "preview": False,
                "result": {
                    "_time": (datetime.now(UTC) - timedelta(seconds=1)).isoformat(),
                    "_raw": "simulated Splunk checkout timeout",
                    "_cd": "simulated:1",
                    "host": "simulated-api-1",
                },
            }
        encoded = (json.dumps(payload) + "\n").encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/x-ndjson")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def handle_datadog_discovery(self, raw: bytes) -> None:
        if not self.datadog_authorized():
            self.send_json({"errors": ["invalid Datadog keys"]}, 403)
            return
        request = json.loads(raw or b"{}")
        groups = request.get("group_by") or []
        facet = groups[0].get("facet") if groups else ""
        buckets = (
            [{"by": {"service": "checkout-service"}, "computes": {"c0": 1}}]
            if facet == "service"
            else []
        )
        self.send_json({"data": {"buckets": buckets}, "meta": {}})

    def handle_datadog_fetch(self, raw: bytes) -> None:
        if not self.datadog_authorized():
            self.send_json({"errors": ["invalid Datadog keys"]}, 403)
            return
        request = json.loads(raw or b"{}")
        query = (request.get("filter") or {}).get("query", "")
        if "checkout-service" not in query:
            self.send_json({"errors": ["generated query omitted discovered service"]}, 400)
            return
        self.send_json(
            {
                "data": [
                    {
                        "id": "simulated-datadog-1",
                        "attributes": {
                            "timestamp": (datetime.now(UTC) - timedelta(seconds=1)).isoformat(),
                            "message": "simulated Datadog checkout timeout",
                            "service": "checkout-service",
                        },
                    }
                ],
                "meta": {"page": {}},
            }
        )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=18091)
    args = parser.parse_args()
    ThreadingHTTPServer(("127.0.0.1", args.port), Handler).serve_forever()


if __name__ == "__main__":
    main()
