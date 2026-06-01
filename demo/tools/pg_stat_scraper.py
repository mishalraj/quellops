#!/usr/bin/env python3
"""Scrape pg_stat_statements and POST top slow queries to aidbg change ingest."""

from __future__ import annotations

import json
import os
import sys
import time
import urllib.request

import psycopg2


def main() -> int:
    dsn = os.getenv(
        "DATABASE_URL",
        "postgresql://aidbg:aidbg@postgres.aidbg-demo.svc.cluster.local:5432/aidbg",
    )
    ingest_url = os.getenv(
        "AIDBG_CHANGE_INGEST_URL",
        "http://host.docker.internal:8088/ingest/change",
    )
    entity = os.getenv("PG_STAT_ENTITY", "inventory")
    top_n = int(os.getenv("PG_STAT_TOP_N", "5"))
    min_mean_ms = float(os.getenv("PG_STAT_MIN_MEAN_MS", "5"))

    with psycopg2.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT query, calls, mean_exec_time, total_exec_time
                FROM pg_stat_statements
                WHERE calls > 0
                ORDER BY mean_exec_time DESC
                LIMIT %s
                """,
                (top_n,),
            )
            rows = cur.fetchall()

    posted = 0
    for query, calls, mean_ms, total_ms in rows:
        if float(mean_ms) < min_mean_ms:
            continue
        payload = {
            "kind": "db.slow_query_stat",
            "at_unix_ms": int(time.time() * 1000),
            "entity": entity,
            "namespace": os.getenv("AIDBG_NAMESPACE", "aidbg-demo"),
            "metadata": {
                "query": (query or "")[:512],
                "calls": int(calls),
                "mean_exec_time_ms": round(float(mean_ms), 2),
                "total_exec_time_ms": round(float(total_ms), 2),
            },
        }
        req = urllib.request.Request(
            ingest_url,
            data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            resp.read()
        posted += 1

    print(f"posted {posted} db.slow_query_stat events to {ingest_url}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
