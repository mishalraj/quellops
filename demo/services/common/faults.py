from __future__ import annotations

import asyncio
import os
import random


def fault_latency_ms() -> int:
    return int(os.getenv("FAULT_LATENCY_MS", "0") or "0")


def fault_error_rate() -> float:
    try:
        return float(os.getenv("FAULT_ERROR_RATE", "0") or "0")
    except ValueError:
        return 0.0


def downstream_url() -> str | None:
    url = (os.getenv("DOWNSTREAM_URL") or "").strip()
    return url or None


async def apply_faults() -> bool:
    """Sleep for configured latency. Return False if this request should error."""
    delay_ms = fault_latency_ms()
    if delay_ms > 0:
        await asyncio.sleep(delay_ms / 1000.0)
    rate = fault_error_rate()
    if rate > 0 and random.random() < rate:
        return False
    return True
