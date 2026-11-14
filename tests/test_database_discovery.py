import unittest

from aidbg.database_connectors import DatabaseSafetyError
from aidbg.database_discovery import discover_cloud_resources


class DatabaseDiscoveryTests(unittest.TestCase):
    def test_injected_provider_resources_are_normalized_and_bounded(self):
        resources = discover_cloud_resources(
            "aws",
            "rds-postgresql",
            {
                "test_resources": [
                    {"id": "orders-ro", "name": "Orders replica", "region": "us-east-1"}
                ]
            },
            {},
        )
        self.assertEqual(
            resources, [{"id": "orders-ro", "name": "Orders replica", "region": "us-east-1"}]
        )

    def test_self_hosted_discovery_is_rejected(self):
        with self.assertRaises(DatabaseSafetyError):
            discover_cloud_resources("self_hosted", "postgresql", {}, {})


if __name__ == "__main__":
    unittest.main()
