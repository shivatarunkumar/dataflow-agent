"""Deterministic error digest: turn raw error texts (tracebacks, Google API errors, Dataflow job messages)
into a short, ranked list of the errors that actually happened. No LLM involved.

Why: the real cause of a pipeline failure is usually at the BOTTOM of a long traceback, often inside an embedded
JSON API error ("Access Denied: ... Permission bigquery.tables.create denied ..."). A model reading truncated
framework frames will guess. The digest hands it (and the signature rules) the facts.
"""

import re
from collections import OrderedDict

# Last line(s) of a Python traceback: "pkg.module.SomeError: message" (message optional).
_EXC_LINE = re.compile(r"^\s*([A-Za-z_][\w.]*(?:Error|Exception|Exit|Interrupt|Denied|Forbidden))(?::\s*(.*))?\s*$", re.MULTILINE)
_HTTP_ERROR_CLASS = re.compile(r"\b(Http[A-Za-z]*Error|HttpForbiddenError|Forbidden|NotFound|BadRequest|Conflict|TooManyRequests)\b")
_JSON_CODE = re.compile(r'"code"\s*:\s*(\d{3})')
_PY_STATUS = re.compile(r"'status'\s*:\s*'(\d{3})'")
_HTTP_WORD = re.compile(r"\b(?:HTTP|status code|Error code)\s*:?\s*(\d{3})\b", re.IGNORECASE)
_GRPC_STATUS = re.compile(r'"status"\s*:\s*"([A-Z_]{4,})"|\b(PERMISSION_DENIED|NOT_FOUND|RESOURCE_EXHAUSTED|UNAVAILABLE|DEADLINE_EXCEEDED|'
                          r"INVALID_ARGUMENT|FAILED_PRECONDITION|ALREADY_EXISTS|UNAUTHENTICATED|INTERNAL|QUOTA_EXCEEDED)\b")
_REASON = re.compile(r'"reason"\s*:\s*"(\w+)"')
_MESSAGE = re.compile(r'"message"\s*:\s*"((?:[^"\\]|\\.){5,600})"')
_PERMISSION = re.compile(r"[Pp]ermission '?([a-z][\w-]*\.[\w.]+)'? (?:denied|is required)(?: on ([\w ]*?\S+:[\w.\-/:]+|[\w ]*?projects/\S+))?")
_RESOURCE = re.compile(r"\b(?:Dataset|Table|Bucket|dataset|table|bucket)\s+([\w\-]+[:.][\w.\-/]+)")
_STEP = re.compile(r"Causes:\s*(S\d+):([^\s,]+)")
_TRACEBACK_SPLIT = re.compile(r"(?=Traceback \(most recent call last\):)")

TRANSIENT_STATUSES = {"UNAVAILABLE", "DEADLINE_EXCEEDED", "INTERNAL", "ABORTED"}


def clip(text: str, max_chars: int = 6000, head: int = 800) -> str:
    """Keep the head AND the tail: tracebacks put the real exception at the end."""
    if not isinstance(text, str) or len(text) <= max_chars:
        return text
    head = min(head, max_chars // 4)
    return text[:head] + f"\n…[{len(text) - max_chars} chars clipped]…\n" + text[-(max_chars - head):]


def iter_strings(data, depth: int = 0):
    """All strings inside nested dict/list evidence data."""
    if depth > 8:
        return
    if isinstance(data, str):
        yield data
    elif isinstance(data, dict):
        for v in data.values():
            yield from iter_strings(v, depth + 1)
    elif isinstance(data, (list, tuple)):
        for v in data:
            yield from iter_strings(v, depth + 1)


def _unescape(text: str) -> str:
    # Log payloads are often JSON-encoded once or twice.
    for _ in range(2):
        if "\\n" in text or '\\"' in text:
            text = text.replace("\\\\", "\\").replace("\\n", "\n").replace('\\"', '"').replace("\\'", "'")
    return text


def _classify(entry: dict) -> str:
    status, http, text = entry.get("status") or "", entry.get("http_code"), entry["text_lower"]
    if status == "PERMISSION_DENIED" or http in (401, 403) or "access denied" in text or entry.get("permission"):
        return "permission"
    if status in ("RESOURCE_EXHAUSTED", "QUOTA_EXCEEDED") or http == 429 or "quota" in text and "exceeded" in text \
            or entry.get("reason") in ("quotaExceeded", "rateLimitExceeded"):
        return "quota"
    if status == "NOT_FOUND" or http == 404 or entry.get("reason") == "notFound" or "not found:" in text:
        return "not_found"
    if "unicodedecodeerror" in text or "unicodeencodeerror" in text or "codec can't" in text:
        return "encoding"
    if "schema" in text and any(w in text for w in ("does not match", "no such field", "invalid", "mismatch")):
        return "schema"
    if status in TRANSIENT_STATUSES or (http and 500 <= http < 600) or "connection reset" in text:
        return "transient"
    return "exception"


_SPECIFICITY = {"permission": 0, "not_found": 1, "schema": 2, "encoding": 3, "quota": 4, "transient": 5, "exception": 6}


def _analyse_block(block: str) -> dict | None:
    """One traceback / message → digest entry (None if there is no error in it)."""
    exc_lines = [(m.group(1), (m.group(2) or "").strip()) for m in _EXC_LINE.finditer(block)]
    http_codes = [int(c) for c in _JSON_CODE.findall(block) + _PY_STATUS.findall(block) + _HTTP_WORD.findall(block)]
    statuses = [a or b for a, b in _GRPC_STATUS.findall(block)]
    messages = [m.replace('\\"', '"') for m in _MESSAGE.findall(block)]
    perm = _PERMISSION.search(block)
    step = _STEP.search(block)
    if not (exc_lines or http_codes or statuses or perm or step):
        return None

    # Exception chain: the last exception is the one raised; a wrapped API error (HttpForbiddenError) is the cause.
    chain = [name.rsplit(".", 1)[-1] for name, _ in exc_lines]
    http_class = _HTTP_ERROR_CLASS.search(block)
    if http_class and http_class.group(1) not in chain:
        chain.append(http_class.group(1))
    last_msg = exc_lines[-1][1] if exc_lines else ""
    message = messages[0] if messages else (last_msg or (step and f"Dataflow step {step.group(2)} failed") or block.strip()[-300:])

    entry = {
        "exception_chain": list(OrderedDict.fromkeys(chain)),
        "message": " ".join(message.split())[:500],
        "http_code": http_codes[0] if http_codes else None,
        "status": next((s for s in statuses if s != "INTERNAL"), statuses[0] if statuses else None),
        "reason": (_REASON.search(block) or [None, None])[1] if _REASON.search(block) else None,
        "permission": perm.group(1) if perm else None,
        "resource": (perm.group(2) if perm and perm.group(2) else None) or (_RESOURCE.search(block).group(1) if _RESOURCE.search(block) else None),
        "step": step.group(2)[:200] if step else None,
        "text_lower": block.lower(),
    }
    if entry["status"] is None and entry["http_code"] == 403:
        entry["status"] = "PERMISSION_DENIED"
    entry["kind"] = _classify(entry)
    # A Dataflow "Workflow failed. Causes: ..." line with no exception is only a wrapper (gives us the step).
    entry["wrapper_only"] = bool(step) and not exc_lines and not http_codes and not perm
    return entry


def build_digest(sources: list, max_entries: int = 6) -> dict:
    """sources: list of (ref, data) where data is any nested evidence payload (or text).

    Returns {"primary": entry | None, "entries": [...], "step": str | None, "text": normalised digest text}.
    """
    groups: "OrderedDict[str, dict]" = OrderedDict()
    step = None
    for ref, data in sources:
        for raw in iter_strings(data):
            if len(raw) < 20:
                continue
            text = _unescape(raw)
            for block in _TRACEBACK_SPLIT.split(text):
                entry = _analyse_block(block)
                if entry is None:
                    continue
                step = step or entry["step"]
                if entry["wrapper_only"]:
                    continue
                key = f"{entry['kind']}|{'>'.join(entry['exception_chain'])}|{entry['message'][:160]}"
                if key in groups:
                    groups[key]["count"] += 1
                    if ref not in groups[key]["sources"]:
                        groups[key]["sources"].append(ref)
                else:
                    entry.pop("text_lower", None)
                    entry.pop("wrapper_only", None)
                    groups[key] = {**entry, "count": 1, "sources": [ref]}

    entries = sorted(groups.values(), key=lambda e: (_SPECIFICITY[e["kind"]], -e["count"]))[:max_entries]
    for e in entries:
        e["step"] = e["step"] or step
    primary = entries[0] if entries else None
    return {"primary": primary, "entries": entries, "step": step, "text": digest_text(entries, step)}


def describe(entry: dict) -> str:
    parts = [entry["kind"].upper()]
    if entry.get("exception_chain"):
        parts.append(" → ".join(entry["exception_chain"]))
    if entry.get("http_code"):
        parts.append(f"http={entry['http_code']}")
    if entry.get("status"):
        parts.append(entry["status"])
    if entry.get("reason"):
        parts.append(f"reason={entry['reason']}")
    if entry.get("permission"):
        parts.append(f"permission={entry['permission']}")
    if entry.get("resource"):
        parts.append(f"resource={entry['resource']}")
    head = " ".join(parts)
    count = f" (x{entry['count']})" if entry.get("count", 1) > 1 else ""
    return f"{head}{count}: {entry['message']}"


def digest_text(entries: list, step: str | None) -> str:
    lines = [f"{i + 1}. {describe(e)} [{', '.join(e['sources'])}]" for i, e in enumerate(entries)]
    if step:
        lines.append(f"Failing Dataflow step: {step}")
    return "\n".join(lines) if lines else "no error found in signal evidence"
