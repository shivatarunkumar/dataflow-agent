import json


def event_brief(event: dict) -> str:
    """Compact event description for prompts."""
    return json.dumps(
        {k: event.get(k) for k in ("incident_id", "status", "title", "facts", "links")}, default=str, indent=1
    )


def evidence_text(evidence: list, max_chars: int, include_data: bool = True) -> str:
    """Evidence for prompts: summaries first (all items), then data until the budget is used."""
    lines = [f"[{e['id']}] ({e['kind']}) {e['summary']}" for e in evidence]
    text = "\n".join(lines)
    if include_data:
        for e in evidence:
            if e["kind"] == "summary" or not e.get("data"):
                continue
            chunk = f"\n\n[{e['id']}] data: {json.dumps(e['data'], default=str)}"
            if len(text) + len(chunk) > max_chars:
                break
            text += chunk
    return text[:max_chars]


CATEGORY_GUIDE = """Root-cause categories:
- TRANSIENT_INFRA: temporary platform problem (quota exhausted, VM could not start, internal/5xx errors, timeouts). A retry should work.
- DATA_QUALITY: the input data/file violates the contract. A retry of the same input will fail again.
- CODE_BUG: a defect in the pipeline code (exceptions/tracebacks from our code, wrong results).
- CONFIG: infrastructure or configuration problem (permissions, missing template/image, limits set too low).
- UNKNOWN: evidence is insufficient."""
