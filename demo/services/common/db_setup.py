from __future__ import annotations

import os

import psycopg2
from opentelemetry.instrumentation.psycopg2 import Psycopg2Instrumentor


def db_dsn() -> str:
    return os.getenv(
        "DATABASE_URL",
        "postgresql://aidbg:aidbg@postgres.quellops-demo.svc.cluster.local:5432/aidbg",
    )


def connect():
    return psycopg2.connect(db_dsn())


def setup_db_otel() -> None:
    Psycopg2Instrumentor().instrument()
