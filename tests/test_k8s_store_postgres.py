from __future__ import annotations

import os
import time
import unittest
import uuid

from aidbg.k8s_store import PostgresKubernetesStore

DATABASE_URL = os.getenv("AIDBG_TEST_POSTGRES_URL", "")


@unittest.skipUnless(DATABASE_URL, "AIDBG_TEST_POSTGRES_URL is not configured")
class PostgresKubernetesStoreTests(unittest.TestCase):
    def setUp(self):
        self.store = PostgresKubernetesStore(DATABASE_URL)
        self.store.initialize()
        self.cluster_id = "test:" + uuid.uuid4().hex

    def tearDown(self):
        self.store.delete(self.cluster_id)

    def test_shared_cluster_scope_and_idempotent_events(self):
        self.store.register(
            {
                "cluster_id": self.cluster_id,
                "name": "replica-test",
                "agent_version": "test",
                "kubernetes_version": "v1.test",
                "namespaces": ["production"],
                "workloads": [],
                "queue_depth": 0,
                "dropped_events": 0,
            }
        )
        self.store.update_scope(self.cluster_id, ["production"])
        event = {
            "event_id": "event-0001",
            "type": "k8s.event",
            "at_unix_ms": int(time.time() * 1000),
            "namespace": "production",
            "name": "checkout",
            "kind": "Deployment",
            "reason": "Unhealthy",
            "message": "readiness probe failed",
            "labels": {"app": "checkout"},
            "fields": {},
        }
        self.assertEqual(self.store.add_events(self.cluster_id, [event]), (1, 0))
        self.assertEqual(self.store.add_events(self.cluster_id, [event]), (0, 1))

        replica = PostgresKubernetesStore(DATABASE_URL)
        replica.initialize()
        self.assertEqual(replica.get(self.cluster_id)["selected_namespaces"], ["production"])
        self.assertEqual(replica.list_events(self.cluster_id)[0]["reason"], "Unhealthy")

        self.assertEqual(self.store.count_transport_backlog(), 1)
        first_claim = self.store.claim_transport_batch("replica-a")
        self.assertEqual(len(first_claim), 1)
        self.assertEqual(replica.claim_transport_batch("replica-b"), [])
        self.store.release_transport("replica-a", [first_claim[0]["id"]], "simulated retry")
        retry_claim = replica.claim_transport_batch("replica-b")
        self.assertEqual(len(retry_claim), 1)
        replica.mark_transport_complete("replica-b", [retry_claim[0]["id"]])
        self.assertEqual(self.store.count_transport_backlog(), 0)


if __name__ == "__main__":
    unittest.main()
