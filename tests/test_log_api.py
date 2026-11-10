import sys
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "backend" / "src"))

from fastapi import FastAPI
from fastapi.testclient import TestClient

from aidbg.log_api import router


class LogAPITests(unittest.TestCase):
    def test_disabled_without_grpc_configuration(self):
        app = FastAPI()
        app.include_router(router("", "", ""))
        with TestClient(app) as client:
            self.assertEqual(client.get("/logs/sources").status_code, 503)

    @patch("aidbg.log_api.LogClient")
    def test_auth_and_grpc_delegation(self, client_type):
        grpc_client = Mock()
        grpc_client.list_sources.return_value = {
            "sources": [{"name": "prod", "provider": "loki"}]
        }
        grpc_client.fetch.return_value = {"records": []}
        grpc_client.investigate.return_value = {
            "strategy": "expand_backwards",
            "records": [],
        }
        client_type.return_value = grpc_client
        app = FastAPI()
        app.include_router(router("logs:8091", "service-secret", "api-secret"))
        headers = {"Authorization": "Bearer api-secret"}
        with TestClient(app) as client:
            self.assertEqual(client.get("/logs/sources").status_code, 401)
            self.assertEqual(client.get("/logs/sources", headers=headers).status_code, 200)
            self.assertEqual(
                client.post(
                    "/logs/fetch",
                    headers=headers,
                    json={"source": "prod", "start": 1000, "end": 1060, "limit": 25},
                ).status_code,
                200,
            )
            self.assertEqual(
                client.post(
                    "/logs/investigate",
                    headers=headers,
                    json={"source": "prod", "start": 1000, "end": 1060},
                ).status_code,
                200,
            )
        grpc_client.fetch.assert_called_once_with("prod", 1000.0, 1060.0, 25)
        grpc_client.investigate.assert_called_once_with("prod", 1000.0, 1060.0, None)

    def test_backend_starts_without_logs_service(self):
        from aidbg.api import create_app
        from aidbg.config import Settings
        from aidbg.ingest_otlp import StatsStore

        app = create_app(StatsStore(), Settings())
        with TestClient(app) as client:
            self.assertEqual(client.get("/healthz").json(), {"ok": True})
            self.assertEqual(client.get("/logs/sources").status_code, 503)


if __name__ == "__main__":
    unittest.main()
