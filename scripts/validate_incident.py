from __future__ import annotations

import json
import sys


def main() -> int:
    if len(sys.argv) != 2:
        print("usage: validate_incident.py <incident.json>")
        return 2
    p = sys.argv[1]
    d = json.load(open(p, "r", encoding="utf-8"))
    required = {"generated_at_unix_ms", "anomalies", "top_causes", "confidence", "graph_snapshot"}
    missing = required - set(d.keys())
    if missing:
        print(f"ERROR: incident missing keys: {sorted(missing)}")
        return 1
    if not isinstance(d["anomalies"], list) or not isinstance(d["top_causes"], list):
        print("ERROR: anomalies/top_causes must be lists")
        return 1
    if not (0.0 <= float(d["confidence"]) <= 1.0):
        print("ERROR: confidence out of range")
        return 1
    print("incident validation passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
