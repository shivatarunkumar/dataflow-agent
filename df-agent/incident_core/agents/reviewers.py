"""Code-review agent and action-review agent."""

import json
import subprocess

from incident_core.agents.common import event_brief, evidence_text
from incident_core.llm import get_llm, structured
from incident_core.models import AUTO_APPROVED, NEEDS_HUMAN, ActionReviewOut, CodeReviewOut
from incident_core.policy import PolicyEngine
from incident_core.trace import tracer


def git_apply_check(repo_root: str, patch: str) -> dict:
    """`git apply --check` only validates; it never modifies the working tree."""
    try:
        proc = subprocess.run(
            ["git", "apply", "--check", "-"], input=patch, text=True, capture_output=True, cwd=repo_root, timeout=30
        )
        return {"applies": proc.returncode == 0, "output": (proc.stderr or proc.stdout)[:2000]}
    except Exception as exc:
        return {"applies": False, "output": f"git apply --check could not run: {exc}"}


def make_code_review_node(cfg: dict, pack):
    def node(state: dict) -> dict:
        reviews = []
        llm = get_llm(cfg, "code_reviewer")
        for action in state.get("plan", []):
            if action["type"] != "PROPOSE_CODE_CHANGE":
                continue
            patch = action.get("patch")
            check = git_apply_check(pack.repo_root(), patch) if patch and pack.repo_root() else None
            out = structured(
                llm,
                CodeReviewOut,
                "You are a strict code reviewer. Judge whether the proposed change fixes the root cause with minimal "
                "scope, has tests, and does not delete data, files or infrastructure. If only a description is given, "
                "judge whether it is specific enough to implement (otherwise INSUFFICIENT).",
                f"Root cause:\n{json.dumps(state['root_cause'], default=str)}\n\n"
                f"Proposal:\n{action['description']}\nSteps: {action.get('steps')}\n\n"
                f"Patch:\n{patch or '(none - description only)'}\n\n"
                f"git apply --check: {json.dumps(check) if check else 'n/a'}",
                role="code_reviewer",
                purpose="review_change",
            )
            review = out.model_dump() if out else {"verdict": "INSUFFICIENT", "comments": ["LLM review unavailable"], "risk": "MEDIUM"}
            if check and not check["applies"]:
                review["verdict"] = "REQUEST_CHANGES"
                review["comments"] = [f"patch does not apply: {check['output']}"] + review["comments"]
            reviews.append({"action_type": action["type"], "git_apply_check": check, **review})
            tracer().event("code_reviewer", "decision",
                           f"{review['verdict']} (risk {review['risk']}"
                           f"{', git apply ok' if check and check['applies'] else ''}): {'; '.join(review['comments'])[:200]}")  # fmt: skip
        return {"code_review": {"reviews": reviews}}

    return node


def make_action_review_node(cfg: dict, brain, pack):
    def node(state: dict) -> dict:
        event, rc = state["event"], state["root_cause"]
        engine = PolicyEngine(brain.policies, pack.config_values())
        decisions, notes = [], []

        # 1) Deterministic policy (authoritative).
        for action in state.get("plan", []):
            facts = {}
            if engine.is_executable(action["type"]):
                try:
                    facts = pack.policy_facts(action, event, rc)
                    tracer().event("action_reviewer", "note", f"policy facts for {action['type']}: {facts}", facts=facts)
                except Exception as exc:
                    notes.append(f"action_review: could not compute policy facts ({exc})")
                    tracer().event("action_reviewer", "error", f"policy facts failed for {action['type']}: {exc}")
            decision = engine.evaluate(action, facts)
            decisions.append(decision)
            tracer().event("action_reviewer", "decision",
                           f"policy: {action['type']} → {decision['decision']} ({'; '.join(decision['reasons'])})",
                           decision=decision)  # fmt: skip

        # 2) LLM second opinion: may only downgrade AUTO_APPROVED -> NEEDS_HUMAN.
        llm = get_llm(cfg, "action_reviewer")
        out = structured(
            llm,
            ActionReviewOut,
            "You review an incident action plan before execution. Reject (approve=false) any action that the evidence "
            "does not justify, that could lose or change data, or that repeats something that already failed.",
            f"Incident:\n{event_brief(event)}\n\nRoot cause:\n{json.dumps(rc, default=str)}\n\n"
            f"Plan:\n{json.dumps(state.get('plan', []), default=str)[:6000]}\n\n"
            f"Evidence:\n{evidence_text(state.get('evidence', []), 6000, include_data=False)}",
            role="action_reviewer",
            purpose="review_plan",
        )
        if out is None and llm is not None:
            notes.append("action_review: LLM review unavailable; policy decision stands")
        concerns = {i.action_type: i for i in (out.items if out else [])}
        for decision in decisions:
            item = concerns.get(decision["action_type"])
            if item is None:
                continue
            decision["llm_review"] = {"approve": item.approve, "concern": item.concern}
            if decision["decision"] == AUTO_APPROVED and not item.approve:
                decision["decision"] = NEEDS_HUMAN
                decision["reasons"].append(f"LLM reviewer concern: {item.concern or 'not justified'}")
                tracer().event("action_reviewer", "decision",
                               f"LLM reviewer downgraded {decision['action_type']} to NEEDS_HUMAN: {item.concern}")  # fmt: skip
            else:
                tracer().event("action_reviewer", "note",
                               f"LLM reviewer on {decision['action_type']}: approve={item.approve} {item.concern}".strip())  # fmt: skip
        return {"decisions": decisions, "notes": notes}

    return node
