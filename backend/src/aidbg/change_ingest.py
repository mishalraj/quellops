from __future__ import annotations

import fnmatch
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml


def load_service_map(path: Path) -> list[dict[str, str]]:
    if not path.is_file():
        return []
    data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    mappings = data.get("mappings") or []
    out: list[dict[str, str]] = []
    for item in mappings:
        if isinstance(item, dict) and item.get("pattern") and item.get("service"):
            out.append({"pattern": str(item["pattern"]), "service": str(item["service"])})
    return out


def services_for_paths(paths: list[str], mappings: list[dict[str, str]]) -> list[str]:
    matched: list[str] = []
    seen: set[str] = set()
    for raw in paths:
        path = raw.lstrip("/")
        for m in mappings:
            if fnmatch.fnmatch(path, m["pattern"]) or fnmatch.fnmatch(raw, m["pattern"]):
                svc = m["service"]
                if svc not in seen:
                    seen.add(svc)
                    matched.append(svc)
    return matched


def _parse_github_timestamp(raw: Any) -> int:
    if raw is None:
        return 0
    if isinstance(raw, (int, float)):
        val = int(raw)
        return val if val > 10_000_000_000 else val * 1000
    text = str(raw).strip()
    if not text:
        return 0
    if text.isdigit():
        val = int(text)
        return val if val > 10_000_000_000 else val * 1000
    try:
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        dt = datetime.fromisoformat(text)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp() * 1000)
    except ValueError:
        return 0


def github_push_to_change_events(
    payload: dict[str, Any], mappings: list[dict[str, str]]
) -> list[dict[str, Any]]:
    if payload.get("deleted"):
        return []
    repo = (payload.get("repository") or {}).get("full_name") or ""
    ref = str(payload.get("ref") or "")
    branch = ref.split("/")[-1] if ref else ""
    head = str(payload.get("after") or "")
    pusher = (payload.get("pusher") or {}).get("name") or ""
    commits = payload.get("commits") or []
    paths: list[str] = []
    for commit in commits:
        if not isinstance(commit, dict):
            continue
        for key in ("added", "modified", "removed"):
            for p in commit.get(key) or []:
                paths.append(str(p))
    services = services_for_paths(paths, mappings)
    if not services and payload.get("head_commit"):
        hc = payload["head_commit"]
        for key in ("added", "modified", "removed"):
            for p in hc.get(key) or []:
                paths.append(str(p))
        services = services_for_paths(paths, mappings)
    at_ms = _parse_github_timestamp((payload.get("head_commit") or {}).get("timestamp"))
    if at_ms <= 0:
        import time

        at_ms = int(time.time() * 1000)
    events: list[dict[str, Any]] = []
    for svc in services or ["unknown"]:
        events.append(
            {
                "kind": "code.push",
                "at_unix_ms": at_ms,
                "entity": svc,
                "namespace": None,
                "metadata": {
                    "repo": repo,
                    "branch": branch,
                    "sha": head[:12] if head else "",
                    "pusher": pusher,
                    "files": paths[:40],
                    "commit_count": len(commits),
                },
            }
        )
    return events
