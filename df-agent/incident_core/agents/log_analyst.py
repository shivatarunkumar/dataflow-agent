"""Log analyst (deterministic, no LLM): error digest → signatures → hypothesis verification.

Runs after the gatherers and before the investigator, so the investigator (and a small model) starts from facts:
the primary error, where it happened, which known failure it matches, and whether that was verified against
the live system.
"""

import json
import logging
import time

from incident_core.brain import Brain
from incident_core.errors import build_digest, describe
from incident_core.models import evidence
from incident_core.signatures import match_signatures
from incident_core.trace import tracer


def signal_sources(event: dict, ev: list) -> list:
    """(ref, data) pairs that describe THIS failure: the event's error text + signal evidence."""
    return [("event", event.get("text", ""))] + [(e["id"], e.get("data")) for e in ev if e.get("signal") and e["kind"] != "error_digest"]


def make_log_analyst_node(cfg: dict, brain: Brain, pack):
    max_chars = cfg["limits"]["evidence_item_max_chars"]

    def node(state: dict) -> dict:
        event, ev = state["event"], state.get("evidence", [])
        sources = signal_sources(event, ev)
        digest = build_digest(sources)
        new_items = []

        if digest["entries"]:
            tracer().event("log_analyst", "note", f"PRIMARY ERROR: {describe(digest['primary'])[:300]}", primary=digest["primary"])
            for line in digest["text"].splitlines()[1:]:
                tracer().event("log_analyst", "note", f"also: {line[:250]}")
        else:
            tracer().event("log_analyst", "note", "no error lines found in this job's signal evidence")

        digest_id = f"analysis-{len([e for e in ev if e['source'] == 'analysis']) + 1}"
        new_items.append({**evidence("analysis", int(digest_id.split("-")[1]), "error_digest",
                                     digest["text"].splitlines()[0] if digest["entries"] else "no errors found", digest, max_chars),
                          "signal": True})  # fmt: skip

        # Signatures see the normalised digest too (e.g. "http=403 PERMISSION_DENIED permission=...").
        match_sources = [(digest_id, digest["text"])] + [(ref, json.dumps(data, default=str) if not isinstance(data, str) else data)
                                                         for ref, data in sources]  # fmt: skip
        matches = match_signatures(brain.signatures, event.get("facts", {}), match_sources)
        if matches:
            tracer().event("log_analyst", "signature", "signatures matched: " + ", ".join(
                f"{m['id']}→{m['category']} ({m['confidence']}, in {m['matched_in']})" for m in matches), matches=matches)  # fmt: skip
        else:
            tracer().event("log_analyst", "signature", "no signature matched")

        verification, calls_made = None, []
        rank = {"CONFIRMED": 2, "REFUTED": 2, "INCONCLUSIVE": 1}
        top = matches[0] if matches else None
        done = {(c["tool"], json.dumps(c["args"], sort_keys=True)) for c in state.get("verifications", [])}
        if top:
            tools = pack.all_tools()
            for name, kwargs in pack.verification_calls(top, digest, event, ev):
                key = (name, json.dumps(kwargs, sort_keys=True))
                if key in done or name not in tools:
                    continue
                done.add(key)
                t0 = time.monotonic()
                idx = len([e for e in ev if e["source"] == "verify"]) + len([i for i in new_items if i["source"] == "verify"]) + 1
                try:
                    summary, data = tools[name].func(**kwargs)
                    item = evidence("verify", idx, name, summary, data, max_chars)
                    verdict = (data or {}).get("verdict", "INCONCLUSIVE") if isinstance(data, dict) else "INCONCLUSIVE"
                    tracer().event("log_analyst", "tool", f"VERIFY {name}({kwargs}) → {verdict}: {summary[:200]}",
                                   time.monotonic() - t0, verdict=verdict)  # fmt: skip
                except Exception as exc:
                    item = evidence("verify", idx, "error", f"verification {name}({kwargs}) failed: {exc}", {})
                    verdict = "INCONCLUSIVE"
                    tracer().event("log_analyst", "tool", f"VERIFY {name}({kwargs}) FAILED: {exc}", time.monotonic() - t0,
                                   level=logging.WARNING)  # fmt: skip
                item["signal"] = False
                new_items.append(item)
                calls_made.append({"tool": name, "args": kwargs})
                result = {"tool": name, "args": kwargs, "verdict": verdict, "summary": item["summary"], "evidence_id": item["id"]}
                if verification is None or rank[verdict] > rank[verification["verdict"]]:
                    verification = result

            # A verified hypothesis is near-certain; a refuted one must not decide the category.
            if verification and verification["verdict"] == "CONFIRMED":
                top["confidence"] = max(top["confidence"], 0.97)
                top["verified"] = verification
            elif verification and verification["verdict"] == "REFUTED":
                top["confidence"] = min(top["confidence"], 0.5)
                top["verified"] = verification
                matches.sort(key=lambda m: m["confidence"], reverse=True)

        return {
            "evidence": new_items,
            "error_digest": digest,
            "signature_matches": matches,
            "verifications": state.get("verifications", []) + calls_made,
            "verification": verification or state.get("verification"),
        }

    return node
