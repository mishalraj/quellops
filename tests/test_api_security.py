from __future__ import annotations

import hashlib
import hmac
import json
import tempfile
import unittest
from pathlib import Path

from aidbg.api import create_app
from aidbg.config import Settings
from aidbg.ingest_otlp import StatsStore
from fastapi.testclient import TestClient


class APISecurityTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.settings = Settings(
            control_db_path=str(Path(self.temp.name) / "control.db"),
            log_admin_token="admin-secret",
            change_ingest_token="change-secret",
            github_webhook_secret="github-secret",
            k8s_agent_token="agent-secret",
        )
        self.client = TestClient(create_app(StatsStore(), self.settings))

    def tearDown(self):
        self.client.close()
        self.temp.cleanup()

    def test_operational_data_requires_administrator_token(self):
        paths = [
            "/stats/ingest",
            "/changes/recent",
            "/stats/services",
            "/graph/snapshot",
            "/trace/focus?service=checkout",
            "/anomalies/latest",
            "/incident/latest",
            "/explain/latest",
            "/k8s/events",
            "/query/why_slow?service=checkout",
            "/query/evidence?service=checkout",
        ]
        for path in paths:
            with self.subTest(path=path):
                self.assertEqual(self.client.get(path).status_code, 401)
                response = self.client.get(path, headers={"Authorization": "Bearer admin-secret"})
                self.assertEqual(response.status_code, 200, response.text)

    def test_direct_change_ingestion_requires_separate_token(self):
        payload = {"kind": "deploy", "entity": "checkout"}
        self.assertEqual(self.client.post("/ingest/change", json=payload).status_code, 401)
        self.assertEqual(
            self.client.post(
                "/ingest/change",
                json=payload,
                headers={"Authorization": "Bearer change-secret"},
            ).status_code,
            200,
        )

    def test_github_webhook_rejects_bad_signature_and_accepts_valid_signature(self):
        body = json.dumps({"zen": "keep it logically awesome"}).encode()
        self.assertEqual(
            self.client.post(
                "/webhook/github",
                content=body,
                headers={"content-type": "application/json", "x-hub-signature-256": "sha256=bad"},
            ).status_code,
            401,
        )
        signature = "sha256=" + hmac.new(b"github-secret", body, hashlib.sha256).hexdigest()
        response = self.client.post(
            "/webhook/github",
            content=body,
            headers={"content-type": "application/json", "x-hub-signature-256": signature},
        )
        self.assertEqual(response.status_code, 200, response.text)
        self.assertTrue(response.json()["ping"])


if __name__ == "__main__":
    unittest.main()
