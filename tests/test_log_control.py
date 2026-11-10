import base64
import os
import sqlite3
import sys
import tempfile
import unittest
from contextlib import closing
from pathlib import Path
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "backend" / "src"))

from fastapi import FastAPI
from fastapi.testclient import TestClient

from aidbg.log_store import LogSourceStore


def master_key() -> str:
    return base64.b64encode(os.urandom(32)).decode("ascii")


class LogStoreTests(unittest.TestCase):
    def test_existing_database_gets_service_column(self):
        with tempfile.TemporaryDirectory() as directory:
            path = str(Path(directory) / "control.db")
            with closing(sqlite3.connect(path)) as connection:
                connection.execute(
                    """
                    CREATE TABLE log_sources (
                        name TEXT PRIMARY KEY,
                        provider TEXT NOT NULL,
                        config_json TEXT NOT NULL,
                        secret_nonce BLOB NOT NULL,
                        secret_ciphertext BLOB NOT NULL,
                        created_at REAL NOT NULL,
                        updated_at REAL NOT NULL
                    )
                    """
                )
            LogSourceStore(path, master_key()).initialize()
            with closing(sqlite3.connect(path)) as connection:
                columns = {
                    row[1]
                    for row in connection.execute("PRAGMA table_info(log_sources)")
                }
            self.assertIn("service", columns)

    def test_credentials_are_encrypted_and_redacted_from_public_results(self):
        with tempfile.TemporaryDirectory() as directory:
            path = str(Path(directory) / "control.db")
            store = LogSourceStore(path, master_key())
            store.initialize()
            public = store.upsert(
                "prod",
                "loki",
                {"url": "https://loki.example.com", "query": "{app=\"api\"}"},
                {"token": "plain-secret"},
                service="checkout",
            )
            self.assertEqual(public["service"], "checkout")
            self.assertEqual(public["credential_keys"], ["token"])
            self.assertNotIn("credentials", public)
            self.assertEqual(store.get("prod")["credentials"]["token"], "plain-secret")
            with closing(sqlite3.connect(path)) as connection:
                row = connection.execute(
                    "SELECT secret_nonce, secret_ciphertext FROM log_sources"
                ).fetchone()
            self.assertNotIn(b"plain-secret", row[0] + row[1])
            self.assertNotIn("plain-secret", Path(path).read_bytes().decode("latin1"))

    def test_invalid_master_key_is_rejected(self):
        with self.assertRaises(ValueError):
            LogSourceStore("unused.db", "not-a-32-byte-base64-key")


class LogControlAPITests(unittest.TestCase):
    @patch("aidbg.log_control.LogClient")
    def test_save_edit_test_delete_and_restart_replay(self, client_type):
        from aidbg.log_control import router

        grpc_client = Mock()
        grpc_client.list_sources.return_value = {"sources": []}
        grpc_client.apply_source.return_value = {"source": {"name": "prod"}}
        grpc_client.test_source.return_value = {
            "ok": True,
            "provider": "loki",
            "records": 1,
            "requests": 1,
            "bytes_received": 100,
        }
        grpc_client.discover.return_value = {
            "source": "prod",
            "provider": "loki",
            "scopes": [{"kind": "service", "key": "service", "value": "checkout", "label": "checkout · service"}],
            "warnings": [],
            "requests": 2,
        }
        grpc_client.delete_source.return_value = {"deleted": True}
        client_type.return_value = grpc_client
        with tempfile.TemporaryDirectory() as directory:
            path = str(Path(directory) / "control.db")
            key = master_key()
            app = FastAPI()
            app.include_router(router("logs:8091", "service", "admin", key, path))
            headers = {"Authorization": "Bearer admin"}
            payload = {
                "provider": "loki",
                "service": "checkout",
                "config": {
                    "url": "https://loki.example.com",
                    "query": "{app=\"api\"}",
                },
                "credentials": {"token": "secret-token"},
            }
            with TestClient(app) as http:
                self.assertEqual(http.get("/admin/log-schemas").status_code, 200)
                self.assertEqual(http.get("/admin/log-sources").status_code, 401)
                saved = http.put(
                    "/admin/log-sources/prod", headers=headers, json=payload
                )
                self.assertEqual(saved.status_code, 200, saved.text)
                self.assertEqual(saved.json()["credential_keys"], ["token"])
                self.assertEqual(saved.json()["service"], "checkout")
                self.assertNotIn("secret-token", saved.text)
                discovered = http.post(
                    "/admin/log-sources/prod/discover", headers=headers
                )
                self.assertEqual(discovered.status_code, 200, discovered.text)
                self.assertEqual(discovered.json()["scopes"][0]["value"], "checkout")
                grpc_client.discover.assert_called_once_with("prod")
                payload["config"]["query"] = "{app=\"worker\"}"
                payload["credentials"] = {}
                edited = http.put(
                    "/admin/log-sources/prod", headers=headers, json=payload
                )
                self.assertEqual(edited.status_code, 200, edited.text)
                self.assertEqual(
                    grpc_client.apply_source.call_args.args[3]["token"],
                    "secret-token",
                )
                tested = http.post(
                    "/admin/log-sources/prod/test", headers=headers
                )
                self.assertEqual(tested.json()["records"], 1)
                configured = http.put(
                    "/admin/log-sources/prod/watcher",
                    headers=headers,
                    json={
                        "enabled": False,
                        "interval_s": 30,
                        "lag_s": 5,
                        "initial_lookback_s": 60,
                    },
                )
                self.assertEqual(configured.status_code, 200, configured.text)
                self.assertFalse(configured.json()["enabled"])
                status = http.get(
                    "/admin/log-monitoring-status", headers=headers
                )
                self.assertEqual(status.status_code, 200, status.text)
                self.assertEqual(status.json()["statuses"][0]["status"], "paused")
                self.assertEqual(status.json()["statuses"][0]["source"], "prod")

            replay_client = Mock()
            replay_client.list_sources.return_value = {"sources": []}
            replay_client.apply_source.return_value = {"source": {"name": "prod"}}
            client_type.return_value = replay_client
            restarted = FastAPI()
            restarted.include_router(
                router("logs:8091", "service", "admin", key, path)
            )
            with TestClient(restarted) as http:
                replay_client.apply_source.assert_called_once()
                self.assertEqual(
                    replay_client.apply_source.call_args.args[3]["token"],
                    "secret-token",
                )
                deleted = http.delete(
                    "/admin/log-sources/prod", headers=headers
                )
                self.assertEqual(deleted.json(), {"deleted": True})

    @patch("aidbg.log_control.LogClient")
    def test_rejects_unknown_fields_and_credentials(self, client_type):
        from aidbg.log_control import router

        client_type.return_value = Mock()
        client_type.return_value.list_sources.return_value = {"sources": []}
        with tempfile.TemporaryDirectory() as directory:
            app = FastAPI()
            app.include_router(
                router(
                    "logs:8091",
                    "service",
                    "admin",
                    master_key(),
                    str(Path(directory) / "control.db"),
                )
            )
            headers = {"Authorization": "Bearer admin"}
            with TestClient(app) as http:
                response = http.put(
                    "/admin/log-sources/prod",
                    headers=headers,
                    json={
                        "provider": "loki",
                        "config": {
                            "url": "https://loki.example.com",
                            "query": "x",
                            "unexpected": "value",
                        },
                        "credentials": {"root_password": "no"},
                    },
                )
                self.assertEqual(response.status_code, 422)


if __name__ == "__main__":
    unittest.main()
