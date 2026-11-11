from __future__ import annotations

import json
import os
import time
import urllib.error
import urllib.request

from .db_setup import connect


def _post_change(kind: str, entity: str, metadata: dict) -> None:
    url = (os.getenv("AIDBG_CHANGE_INGEST_URL") or "").strip()
    if not url:
        return
    payload = {
        "kind": kind,
        "at_unix_ms": int(time.time() * 1000),
        "entity": entity,
        "namespace": os.getenv("AIDBG_NAMESPACE", "quellops-demo"),
        "metadata": metadata,
    }
    req = urllib.request.Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            resp.read()
    except urllib.error.URLError as exc:
        print(f"migrate: change ingest skipped ({exc})")


def ensure_schema() -> None:
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS inventory_items (
                  id SERIAL PRIMARY KEY,
                  sku VARCHAR(64) NOT NULL,
                  qty INT NOT NULL
                )
                """
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_inventory_sku
                ON inventory_items (sku)
                """
            )
            cur.execute(
                """
                INSERT INTO inventory_items (sku, qty)
                SELECT v.sku, v.qty
                FROM (VALUES ('widget-a', 120), ('widget-b', 45), ('widget-c', 9)) AS v(sku, qty)
                WHERE NOT EXISTS (SELECT 1 FROM inventory_items LIMIT 1)
                """
            )
        conn.commit()


def apply_migration(name: str) -> None:
    with connect() as conn:
        with conn.cursor() as cur:
            if name == "drop_inventory_sku_index":
                cur.execute("DROP INDEX IF EXISTS idx_inventory_sku")
                statement = "DROP INDEX IF EXISTS idx_inventory_sku"
            elif name == "restore_inventory_sku_index":
                cur.execute(
                    "CREATE INDEX IF NOT EXISTS idx_inventory_sku ON inventory_items (sku)"
                )
                statement = "CREATE INDEX IF NOT EXISTS idx_inventory_sku ON inventory_items (sku)"
            else:
                raise ValueError(f"unknown migration: {name}")
        conn.commit()
    _post_change(
        "db.migration.applied",
        os.getenv("SERVICE_NAME", "inventory"),
        {"migration": name, "statement": statement},
    )


if __name__ == "__main__":
    action = os.getenv("MIGRATION_ACTION", "ensure_schema")
    if action == "ensure_schema":
        ensure_schema()
    else:
        apply_migration(action)
