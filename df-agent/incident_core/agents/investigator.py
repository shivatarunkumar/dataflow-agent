"""Investigator: decides the root cause from FACTS (error digest, signatures, verification), then lets the LLM
explain. The LLM can decide the category only when no strong signature applies, and its answer must pass
consistency guards (df-agent.md §19). This keeps a small model from inventing a cause."""

import json
import logging

from incident_core.agents.common import CATEGORY_GUIDE, event_brief, evidence_text
from incident_core.brain import Brain
from incident_core.errors import describe
from incident_core.llm import get_llm, structured
from incident_core.models import RootCauseOut
from incident_core.trace import tracer

SYSTEM = """You are a senior SRE investigating a failed data-pipeline job. Work like an engineer, not a storyteller.

Rules:
1. The ROOT CAUSE is the most specific error in the ERROR DIGEST: an HTTP status, an IAM permission, or the exception
   at the BOTTOM of a traceback. Beam/Dataflow framework frames and "Workflow failed" are wrappers, not causes.
2. Use ONLY the facts given. Never invent data problems, files, columns or numbers that are not in the evidence.
3. DATA_QUALITY is only possible if validation checks FAILED (job status REJECTED). With no failed checks it is wrong.
4. HTTP 403 / Access Denied / PERMISSION_DENIED = CONFIG (missing IAM permission), not a code bug and not data.
5. If a VERIFIED HYPOTHESIS is given as CONFIRMED, your category must match it.
6. Cite evidence ids (e.g. gcp-2, analysis-1, verify-1) for every claim. Name the exact error, resource and permission.

{categories}

System overview:
{system}"""


def _guard(rc: dict, event: dict, matches: list, digest: dict, valid_ids: set) -> list:
    """Applies consistency guards in place; returns notes describing every correction."""
    notes = []
    facts = event.get("facts", {})
    has_failed_checks = bool(facts.get("failed_checks")) or event.get("status") == "REJECTED"
    if rc["category"] == "DATA_QUALITY" and not has_failed_checks:
        notes.append("guard: DATA_QUALITY rejected - status is FAILED and no validation check failed")
        rc["category"], rc["confidence"] = "UNKNOWN", 0.3
    transient_sig = any(m["category"] == "TRANSIENT_INFRA" and m["confidence"] >= 0.7 for m in matches)
    if rc["category"] == "TRANSIENT_INFRA" and not transient_sig and rc["confidence"] > 0.6:
        notes.append("guard: TRANSIENT_INFRA without a transient error signature - confidence capped at 0.6 (no auto-retry)")
        rc["confidence"] = 0.6
    if not rc.get("evidence_refs"):
        notes.append("guard: no valid evidence cited - confidence x0.6")
        rc["confidence"] = round(rc["confidence"] * 0.6, 2)
    return notes


def make_investigator_node(cfg: dict, brain: Brain):
    limits = cfg["limits"]
    decisive_at = float(brain.policies.get("signature_override_confidence", 0.85))

    def node(state: dict) -> dict:
        event, ev = state["event"], state.get("evidence", [])
        loops = state.get("loops", 0)
        digest = state.get("error_digest") or {"entries": [], "text": "no error found", "primary": None}
        matches = state.get("signature_matches") or []
        verification = state.get("verification")
        top = matches[0] if matches else None
        decisive = top if top and top["confidence"] >= decisive_at else None
        runbook_keys = list(dict.fromkeys(([top["runbook"]] if top and top.get("runbook") else []) + event.get("runbook_hints", [])))
        valid_ids = {e["id"] for e in ev}
        top_text = "{}→{} ({}){}".format(top["id"], top["category"], top["confidence"], " [DECISIVE]" if decisive else "") if top else "none"
        verify_text = "{} via {}".format(verification["verdict"], verification["tool"]) if verification else "none"
        tracer().event("investigator", "note",
                       f"evidence {len(ev)} items; top signature: {top_text}; verification: {verify_text}; round {loops + 1}")  # fmt: skip

        facts_block = (
            f"INCIDENT:\n{event_brief(event)}\n\n"
            f"ERROR DIGEST (deterministic, most specific first):\n{digest['text']}\n\n"
            f"VALIDATION: failed checks = {event.get('facts', {}).get('failed_checks') or 'none'}\n\n"
            + (f"VERIFIED HYPOTHESIS: {verification['verdict']} - {verification['summary']} [{verification['evidence_id']}]\n\n" if verification else "")
            + (f"KNOWN FAILURE MATCHED: {top['id']} → {top['category']} (confidence {top['confidence']}"
               f"{', DECIDED - explain it, do not change it' if decisive else ', a hint only'}), matched in {top['matched_in']}\n\n" if top else "")
            + f"RUNBOOK:\n{brain.runbooks(runbook_keys, 3500)}\n\n"
            f"OTHER EVIDENCE (summaries):\n{evidence_text([e for e in ev if e['kind'] != 'error_digest'], 4000, include_data=False)}"
        )  # fmt: skip
        out = structured(
            get_llm(cfg, "investigator"),
            RootCauseOut,
            SYSTEM.format(categories=CATEGORY_GUIDE, system=brain.system[:2500]),
            facts_block + "\n\nReturn the root cause. The summary must name the exact error (type, message, resource, "
                          "permission) and where it happened, citing evidence ids.",
            role="investigator",
            purpose="root_cause",
        )  # fmt: skip

        notes, requests = [], []
        primary = digest.get("primary")
        if primary:
            fact_summary = describe(primary) + (f" (Dataflow step {digest['step']})" if digest.get("step") else "")
        elif event.get("failed_checks"):  # REJECTED: the facts are the failed validation checks
            fact_summary = "Validation failed: " + "; ".join(
                f"{c.get('check_name')}: {c.get('summary')}" for c in event["failed_checks"])
        else:
            fact_summary = ""
        if decisive:
            # Facts decide; the LLM only contributes the explanation if it agrees.
            rc = {"category": decisive["category"], "confidence": decisive["confidence"], "method": "signature",
                  "evidence_refs": [r for r in {decisive["matched_in"], (verification or {}).get("evidence_id")} if r in valid_ids]}  # fmt: skip
            if out is not None and out.category == decisive["category"]:
                rc["summary"] = out.summary
                rc["evidence_refs"] = sorted(set(rc["evidence_refs"]) | {r for r in out.evidence_refs if r in valid_ids})
                rc["method"] = "signature+llm"
            else:
                if out is not None:
                    notes.append(f"investigator: LLM said {out.category}, but the evidence decides {decisive['category']} "
                                 f"(signature '{decisive['id']}'{', verified' if decisive.get('verified') else ''}); LLM explanation discarded")  # fmt: skip
                rc["summary"] = (f"{fact_summary}. " if fact_summary else "") + \
                    f"Matches known failure '{decisive['id']}' ({decisive['category']})." + \
                    (f" Verified: {verification['summary']}" if verification and verification["verdict"] == "CONFIRMED" else "")
        elif out is not None:
            rc = out.model_dump(exclude={"needs_more_evidence"})
            rc["evidence_refs"] = [r for r in rc["evidence_refs"] if r in valid_ids]
            rc["method"] = "llm"
            notes += _guard(rc, event, matches, digest, valid_ids)
            if rc["category"] == "UNKNOWN" and top:
                rc.update(category=top["category"], confidence=min(top["confidence"], 0.6), method="signature (weak)")
                notes.append(f"investigator: fell back to weak signature '{top['id']}'")
            requests = out.needs_more_evidence
        else:
            if top:
                rc = {"category": top["category"], "confidence": top["confidence"], "method": "signature (LLM unavailable)",
                      "evidence_refs": [top["matched_in"]] if top["matched_in"] in valid_ids else [],
                      "summary": (f"{fact_summary}. " if fact_summary else "") + f"Matches known failure '{top['id']}'."}  # fmt: skip
            else:
                rc = {"category": "UNKNOWN", "confidence": 0.3, "method": "fallback", "evidence_refs": [],
                      "summary": (f"Unclassified error: {fact_summary}." if fact_summary else "No error found in the job's logs.")
                      + " A human must investigate."}  # fmt: skip
            notes.append("investigator: LLM output unavailable; used deterministic classification")

        rc.update(signature=top, runbooks=runbook_keys, primary_error=primary, failing_step=digest.get("step"),
                  verification=verification, digest_text=digest.get("text"))  # fmt: skip
        more = bool(requests) and loops < limits["evidence_rounds"] and not decisive
        for note in notes:
            tracer().event("investigator", "note", note, level=logging.WARNING)
        tracer().event("investigator", "decision",
                       f"ROOT CAUSE {rc['category']} (confidence {rc['confidence']:.2f}, via {rc['method']}): {rc['summary'][:220]}"
                       + (f"; requesting more evidence: {requests}" if more else ""),
                       root_cause={k: v for k, v in rc.items() if k != "digest_text"})  # fmt: skip
        return {
            "root_cause": rc,
            "evidence_requests": requests if more else [],
            "loops": loops + (1 if more else 0),
            "notes": notes,
        }

    return node


def route_after_investigation(state: dict):
    return "more_evidence" if state.get("evidence_requests") else "plan"
