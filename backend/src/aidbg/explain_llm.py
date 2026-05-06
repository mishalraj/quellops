from __future__ import annotations

import json
from typing import Any

from .config import Settings


def _strip_markdown_json_fence(content: str) -> str:
    """Models often wrap JSON in ```json ... ``` even when asked for raw JSON only."""
    text = content.strip()
    if not text.startswith("```"):
        return text
    lines = text.split("\n")
    if lines and lines[0].strip().startswith("```"):
        lines = lines[1:]
    if lines and lines[-1].strip() == "```":
        lines = lines[:-1]
    return "\n".join(lines).strip()


def _parse_llm_json_blob(content: str) -> dict[str, Any] | None:
    text = _strip_markdown_json_fence(content)
    try:
        parsed = json.loads(text)
    except Exception:
        return None
    return parsed if isinstance(parsed, dict) else None


def explain_incident(incident: dict[str, Any], settings: Settings) -> dict[str, Any]:
    if (
        not settings.azure_openai_endpoint
        or not settings.azure_openai_api_key
        or not settings.azure_openai_deployment
    ):
        return {
            "provider": "disabled",
            "reason": "missing_azure_openai_env",
            "summary": "LLM explanation is disabled until Azure OpenAI env vars are configured.",
            "ranked_causes": incident.get("top_causes", []),
        }
    try:
        from openai import AzureOpenAI
    except Exception as exc:  # pragma: no cover - dependency/runtime guard
        return {
            "provider": "azure_openai",
            "reason": "openai_package_missing",
            "error": str(exc),
            "ranked_causes": incident.get("top_causes", []),
        }

    client = AzureOpenAI(
        api_version=settings.azure_openai_api_version,
        azure_endpoint=settings.azure_openai_endpoint,
        api_key=settings.azure_openai_api_key,
    )
    prompt = (
        "Given this incident JSON, explain the top likely causes and evidence in a deterministic way. "
        "Return strict JSON only with keys: summary, ranked_causes, evidence_bullets, suggested_fixes, confidence "
        "(confidence is optional 0-1 for how well the narrative fits the JSON; omit if unsure).\n"
        "ranked_causes MUST be a JSON array (not a string). Each element MUST be an object, either "
        '{"cause":"short label","likelihood":"low|medium|high","reasoning":"one or two sentences"} '
        "or {\"type\":\"...\",\"entity\":\"...\",\"reasoning\":\"optional\"} mirroring top_causes. "
        "Do not put a single cause object at the top level instead of an array.\n\n"
        + json.dumps(incident)
    )
    response = client.chat.completions.create(
        model=settings.azure_openai_deployment,
        messages=[
            {
                "role": "system",
                "content": (
                    "You produce concise deterministic JSON only. "
                    "Never wrap the JSON in markdown code fences or add prose before or after the object."
                ),
            },
            {"role": "user", "content": prompt},
        ],
        temperature=0.2,
    )
    content = (response.choices[0].message.content or "").strip()
    payload = _parse_llm_json_blob(content)
    if payload is None:
        payload = {
            "summary": content,
            "ranked_causes": incident.get("top_causes", []),
            "evidence_bullets": [],
            "suggested_fixes": [],
        }
    else:
        payload.setdefault("summary", "")
        payload.setdefault("ranked_causes", incident.get("top_causes", []))
        payload.setdefault("evidence_bullets", [])
        payload.setdefault("suggested_fixes", [])
    payload["provider"] = "azure_openai"
    return payload
