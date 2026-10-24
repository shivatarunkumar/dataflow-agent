"""Deterministic root-cause signatures (brain/signatures.yaml) and key-error extraction."""

import re

# Exception / failure lines worth surfacing to the investigator and the report.
_KEY_ERROR_PATTERNS = [
    re.compile(r"^\s*([A-Z][A-Za-z0-9_.]*(?:Error|Exception)): (.{1,300})$", re.MULTILINE),  # Python exceptions
    re.compile(r"(Workflow failed\. Causes: .{1,300})"),  # Dataflow job failure summary
    re.compile(r"(\[?QUOTA_EXCEEDED\]?.{0,200})"),
    re.compile(r"(RESOURCE_EXHAUSTED.{0,200})"),
    re.compile(r"(PERMISSION_DENIED.{0,200})"),
]


def _when_matches(when: dict, facts: dict) -> bool:
    for key, expected in (when or {}).items():
        if key == "failed_check":
            if expected not in facts.get("failed_checks", []):
                return False
        elif str(facts.get(key)) != str(expected):
            return False
    return True


def match_signatures(signatures: list, facts: dict, sources) -> list:
    """Matches signatures against failure-signal text.

    sources: a string, or a list of (ref, text) pairs (ref = "event" or an evidence id) so every match records
    where its text came from. Returns matches sorted by confidence (highest first).
    """
    if isinstance(sources, str):
        sources = [("event", sources)]
    matches = []
    for sig in signatures:
        if not _when_matches(sig.get("when"), facts):
            continue
        patterns = sig.get("any_text") or []
        hit = ref = None
        if patterns:
            flags = 0 if sig.get("case_sensitive") else re.IGNORECASE
            for pattern in patterns:
                for source_ref, text in sources:
                    m = re.search(pattern, text, flags)
                    if m:
                        hit, ref = text[max(0, m.start() - 80): m.end() + 80], source_ref
                        break
                if hit is not None:
                    break
            if hit is None:
                continue
        matches.append({
            "id": sig["id"],
            "category": sig["category"],
            "confidence": float(sig["confidence"]),
            "runbook": sig.get("runbook"),
            "matched_text": hit,
            "matched_in": ref or "event facts",
            "remediation": sig.get("remediation"),
        })  # fmt: skip
    return sorted(matches, key=lambda m: m["confidence"], reverse=True)


def key_errors(sources, limit: int = 6) -> list:
    """Distinct exception/failure lines from failure-signal text, e.g. "UnicodeDecodeError: 'utf-8' codec ...",
    each as {"error": ..., "source": ref}."""
    if isinstance(sources, str):
        sources = [("event", sources)]
    found, seen = [], set()
    for ref, text in sources:
        text = text.replace("\\n", "\n")  # JSON-encoded log payloads
        for pattern in _KEY_ERROR_PATTERNS:
            for m in pattern.finditer(text):
                line = " ".join(m.group(0).split())[:300]
                key = line[:120]
                if key not in seen:
                    seen.add(key)
                    found.append({"error": line, "source": ref})
    # Exceptions first (most specific), then platform messages.
    found.sort(key=lambda f: 0 if re.match(r"[A-Z][A-Za-z0-9_.]*(Error|Exception):", f["error"]) else 1)
    return found[:limit]
