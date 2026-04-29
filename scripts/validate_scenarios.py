from __future__ import annotations

from pathlib import Path
import sys

import yaml


REQUIRED_KEYS = {"name", "description", "target", "inject", "assert"}
TARGET_KEYS = {"namespace", "deployment"}
INJECT_KEYS = {"type"}


def main() -> int:
    scenarios_dir = Path("scenarios")
    errors: list[str] = []
    for path in sorted(scenarios_dir.glob("*.yaml")):
        data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        missing = REQUIRED_KEYS - set(data.keys())
        if missing:
            errors.append(f"{path}: missing keys {sorted(missing)}")
            continue
        if TARGET_KEYS - set((data.get("target") or {}).keys()):
            errors.append(f"{path}: target must include {sorted(TARGET_KEYS)}")
        if INJECT_KEYS - set((data.get("inject") or {}).keys()):
            errors.append(f"{path}: inject must include {sorted(INJECT_KEYS)}")
    if errors:
        for e in errors:
            print(f"ERROR: {e}")
        return 1
    print("Scenario schema validation passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
