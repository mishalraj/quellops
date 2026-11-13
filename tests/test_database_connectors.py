from __future__ import annotations

import unittest

from aidbg.database_connectors import (
    CDCConnector,
    CloudMetricsConnector,
    DatabaseSafetyError,
    connector_for,
    ensure_select_only,
    fingerprint,
    normalize_metric,
    validate_source,
)


class DatabaseConnectorSafetyTests(unittest.TestCase):
    def test_provider_metrics_are_normalized_and_ranked(self):
        self.assertEqual(normalize_metric("ReplicaLag", 70), ("replica_lag", "critical"))
        self.assertEqual(normalize_metric("CPUUtilization", 85), ("cpu_percent", "warning"))

    def test_replica_mode_enforces_safe_defaults(self):
        config = validate_source(
            "orders-replica",
            "aws",
            "postgresql",
            "replica",
            {"host": "orders-ro.internal", "database": "orders"},
        )
        self.assertTrue(config["require_replica"])
        self.assertTrue(config["read_only"])
        self.assertFalse(config["enabled"])
        self.assertEqual(config["max_connections"], 2)
        self.assertEqual(config["timeout_s"], 5)

    def test_primary_override_is_always_rejected(self):
        with self.assertRaisesRegex(DatabaseSafetyError, "primary"):
            validate_source(
                "orders",
                "self_hosted",
                "postgresql",
                "replica",
                {"host": "primary.internal", "allow_primary": True},
            )

    def test_cloud_and_cdc_modes_do_not_need_database_connection(self):
        cloud = validate_source(
            "rds-prod",
            "aws",
            "rds-postgresql",
            "cloud_metrics",
            {"resource_id": "orders-prod", "region": "us-east-1"},
        )
        cdc = validate_source(
            "orders-cdc",
            "aws",
            "aurora-postgresql",
            "cdc",
            {"bootstrap_servers": "broker:9092", "topic": "orders.public"},
        )
        self.assertEqual(cloud["resource_id"], "orders-prod")
        self.assertFalse(cdc["commit_offsets"])

    def test_bounds_are_hard_limits(self):
        with self.assertRaisesRegex(DatabaseSafetyError, "max_connections"):
            validate_source(
                "unsafe",
                "self_hosted",
                "mysql",
                "replica",
                {"host": "replica", "max_connections": 20},
            )
        with self.assertRaisesRegex(DatabaseSafetyError, "interval_s"):
            validate_source(
                "unsafe",
                "aws",
                "dynamodb",
                "cloud_metrics",
                {"resource_id": "table", "interval_s": 1},
            )

    def test_arbitrary_and_write_sql_is_rejected(self):
        ensure_select_only(["SELECT count(*) FROM pg_stat_activity", "SHOW STATUS"])
        for sql in (
            "DELETE FROM users",
            "SELECT 1; DROP TABLE users",
            "VACUUM users",
            "CALL dangerous()",
        ):
            with self.assertRaises(DatabaseSafetyError, msg=sql):
                ensure_select_only([sql])

    def test_query_fingerprint_removes_values(self):
        self.assertEqual(
            fingerprint("SELECT * FROM orders WHERE id=42"),
            fingerprint("select * from orders where id=99"),
        )

    def test_connector_factory_selects_safe_mode(self):
        replica = connector_for(
            "pg",
            "self_hosted",
            "postgresql",
            "replica",
            {"host": "replica", "database": "postgres"},
            {},
        )
        cloud = connector_for(
            "rds",
            "aws",
            "rds-postgresql",
            "cloud_metrics",
            {"resource_id": "rds", "test_points": []},
            {},
        )
        cdc = connector_for(
            "cdc",
            "gcp",
            "cloudsql-postgresql",
            "cdc",
            {"bootstrap_servers": "kafka:9092", "topic": "db"},
            {},
        )
        self.assertEqual(replica.engine, "postgresql")
        self.assertIsInstance(cloud, CloudMetricsConnector)
        self.assertIsInstance(cdc, CDCConnector)

    def test_cloud_metrics_normalizes_provider_points(self):
        connector = CloudMetricsConnector(
            "prod",
            "aws",
            "dynamodb",
            validate_source(
                "prod",
                "aws",
                "dynamodb",
                "cloud_metrics",
                {
                    "resource_id": "orders",
                    "test_points": [
                        {
                            "metric": "ThrottledRequests",
                            "value": 12,
                            "unit": "count",
                            "severity": "warning",
                        }
                    ],
                },
            ),
            {},
        )
        result = connector.collect()
        self.assertEqual(result.rows, 1)
        self.assertEqual(result.signals[0].signal, "throttled_requests")
        self.assertEqual(result.signals[0].provider, "aws")
        self.assertEqual(result.signals[0].evidence["access_mode"], "cloud_metrics")


if __name__ == "__main__":
    unittest.main()
