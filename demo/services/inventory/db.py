from __future__ import annotations

import os

from services.common.db_setup import connect


def fetch_item(sku: str = "widget-a") -> dict | None:
    sleep_ms = int(os.getenv("FAULT_DB_SLEEP_MS", "0") or "0")
    with connect() as conn:
        with conn.cursor() as cur:
            if sleep_ms > 0:
                cur.execute("SELECT pg_sleep(%s)", (sleep_ms / 1000.0,))
            cur.execute(
                "SELECT id, sku, qty FROM inventory_items WHERE sku = %s",
                (sku,),
            )
            row = cur.fetchone()
    if not row:
        return None
    return {"id": row[0], "sku": row[1], "qty": row[2]}


def list_items(limit: int = 20) -> list[dict]:
    sleep_ms = int(os.getenv("FAULT_DB_SLEEP_MS", "0") or "0")
    with connect() as conn:
        with conn.cursor() as cur:
            if sleep_ms > 0:
                cur.execute("SELECT pg_sleep(%s)", (sleep_ms / 1000.0,))
            cur.execute(
                "SELECT id, sku, qty FROM inventory_items ORDER BY id LIMIT %s",
                (limit,),
            )
            rows = cur.fetchall()
    return [{"id": r[0], "sku": r[1], "qty": r[2]} for r in rows]
