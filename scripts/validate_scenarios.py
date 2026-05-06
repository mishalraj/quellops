from __future__ import annotations

from pathlib import Path
import sys

import yaml


REQUIRED_KEYS = {"name", "description", "target", "inject", "assert"}
TARGET_KEYS = {"namespace", "deployment"}
INJECT_KEYS = {"type"}
ALLOWED_INJECT_TYPES = {"rollout_restart", "annotate_placeholder", "scale_to_zero", "load_spike_only"}


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
        inject_type = (data.get("inject") or {}).get("type")
        if inject_type not in ALLOWED_INJECT_TYPES:
            errors.append(
                f"{path}: inject.type must be one of {sorted(ALLOWED_INJECT_TYPES)}"
            )
        assert_cfg = data.get("assert") or {}
        expect_top_types = assert_cfg.get("expect_top_types")
        not_top_1_type = assert_cfg.get("not_top_1_type")
        allow_top_k = assert_cfg.get("allow_top_k", 3)
        if expect_top_types is not None and not isinstance(expect_top_types, list):
            errors.append(f"{path}: assert.expect_top_types must be a list when present")
        if not_top_1_type is not None and not isinstance(not_top_1_type, str):
            errors.append(f"{path}: assert.not_top_1_type must be a string when present")
        if not isinstance(allow_top_k, int) or allow_top_k <= 0:
            errors.append(f"{path}: assert.allow_top_k must be a positive integer")
        wait_seconds = data.get("wait_seconds")
        if wait_seconds is not None and (
            not isinstance(wait_seconds, int) or wait_seconds <= 0
        ):
            errors.append(f"{path}: wait_seconds must be a positive integer when present")
        if not expect_top_types and not not_top_1_type:
            errors.append(
                f"{path}: assert must define expect_top_types and/or not_top_1_type"
            )
    if errors:
        for e in errors:
            print(f"ERROR: {e}")
        return 1
    print("Scenario schema validation passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
