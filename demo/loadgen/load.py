#!/usr/bin/env python3
"""Generate HTTP traffic against the demo edge service."""
from __future__ import annotations

import argparse
import os
import sys
import time

import httpx


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default=os.getenv("EDGE_URL", "http://localhost:8080/"))
    parser.add_argument("--duration", type=int, default=int(os.getenv("DURATION_SECONDS", "60")))
    parser.add_argument("--rps", type=float, default=float(os.getenv("RPS", "8")))
    args = parser.parse_args()
    interval = 1.0 / max(args.rps, 0.1)
    end = time.time() + args.duration
    ok = err = 0
    while time.time() < end:
        try:
            r = httpx.get(args.url, timeout=30.0)
            if r.is_success:
                ok += 1
            else:
                err += 1
        except Exception:
            err += 1
        time.sleep(interval)
    print(f"load complete ok={ok} err={err} url={args.url}")
    return 0 if ok > 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
