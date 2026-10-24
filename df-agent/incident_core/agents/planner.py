"""Action planner: actions come from the playbook (+ domain code for parameters); the LLM writes the narrative.

Optional (features.generate_patches): drafts a unified diff for PROPOSE_CODE_CHANGE by reading repo files.
"""

import json
from pathlib import Path

from langchain_core.messages import HumanMessage, SystemMessage, ToolMessage

from incident_core.agents.common import event_brief, evidence_text
from incident_core.brain import Brain
from incident_core.llm import get_llm, structured
from incident_core.models import PatchOut, plan_text_model
from incident_core.trace import tracer

_SKIP = {".git", ".venv", ".venv-agent", "__pycache__", ".terraform", "actions", ".state"}


_SENSITIVE = ("tfstate", ".env", "credential", "secret", "key.json")


def _safe_path(root: Path, rel: str) -> Path:
    """Read-only access confined to the repo, excluding venvs, state and anything secret-looking."""
    root = root.resolve()
    path = (root / rel).resolve()
    if path != root and root not in path.parents:
        raise ValueError("path outside repository")
    if any(part in _SKIP for part in path.relative_to(root).parts) or any(s in path.name for s in _SENSITIVE):
        raise ValueError("path not allowed")
    return path


def read_repo_file(root: Path, path: str) -> str:
    return _safe_path(root, path).read_text()[:12000]


def list_repo(root: Path, path: str = ".") -> str:
    base = _safe_path(root, path)
    entries = sorted(p.name + ("/" if p.is_dir() else "") for p in base.iterdir() if p.name not in _SKIP)
    return "\n".join(entries[:200])


def _draft_patch(llm, root: Path, state: dict, action: dict, budget: int = 6):
    tools = [
        {"type": "function", "function": {"name": "read_repo_file", "description": "Read a repository file (relative path).",
                                          "parameters": {"type": "object", "properties": {"path": {"type": "string"}}, "required": ["path"]}}},
        {"type": "function", "function": {"name": "list_repo", "description": "List a repository directory (relative path).",
                                          "parameters": {"type": "object", "properties": {"path": {"type": "string"}}, "required": ["path"]}}},
    ]  # fmt: skip
    rc = state["root_cause"]
    messages = [
        SystemMessage(content="You fix pipeline code. Read only the files you need, then stop calling tools."),
        HumanMessage(content=f"Root cause:\n{json.dumps(rc, default=str)}\n\nProposed change:\n{action['description']}\n\n"
                             f"Evidence:\n{evidence_text(state.get('evidence', []), 6000)}"),
    ]  # fmt: skip
    bound = llm.bind_tools(tools)
    for _ in range(budget):
        response = bound.invoke(messages)
        calls = getattr(response, "tool_calls", None) or []
        if not calls:
            break
        messages.append(response)
        for call in calls:
            try:
                fn = read_repo_file if call["name"] == "read_repo_file" else list_repo
                result = fn(root, **(call.get("args") or {}))
            except Exception as exc:
                result = f"error: {exc}"
            messages.append(ToolMessage(content=result, tool_call_id=call.get("id") or call["name"]))
    context = "\n\n".join(m.content for m in messages if isinstance(m, ToolMessage))[:20000]
    return structured(
        llm,
        PatchOut,
        "Write a minimal, correct unified diff (git format, paths relative to the repo root). Include a unit test if possible. "
        "Never delete files, data or infrastructure.",
        f"Root cause:\n{json.dumps(rc, default=str)}\n\nChange:\n{action['description']}\n\nFiles read:\n{context}",
        role="action_planner",
        purpose="draft_patch",
    )


class _Fill(dict):
    def __missing__(self, key):
        return "(unknown)"


def _apply_remediation(actions: list, rc: dict, context: dict) -> list:
    """Fills signature remediation templates (brain/signatures.yaml) into matching actions. Returns types filled."""
    remediation = (rc.get("signature") or {}).get("remediation") or {}
    filled = []
    values = _Fill({k: ("" if v is None else v) for k, v in context.items()})
    for action in actions:
        template = remediation.get(action["type"])
        if not template:
            continue
        action["description"] = " ".join(template.get("description", action["description"]).format_map(values).split())
        action["steps"] = [s.format_map(values) for s in template.get("steps", [])]
        action["remediation_source"] = f"signature:{rc['signature']['id']}"
        filled.append(action["type"])
    return filled


def make_planner_node(cfg: dict, brain: Brain, pack):
    def node(state: dict) -> dict:
        event, rc = state["event"], state["root_cause"]
        playbook = brain.playbooks.get(rc["category"]) or brain.playbooks["UNKNOWN"]
        actions = pack.build_actions(rc["category"], event, playbook)
        notes = []
        tracer().event("action_planner", "decision",
                       f"playbook {rc['category']} → actions {[a['type'] for a in actions]}",
                       actions=[{"type": a["type"], "params": a.get("params")} for a in actions])  # fmt: skip

        # 1) Deterministic remediation from the matched signature (exact permission/resource/principal), if any.
        remediated = _apply_remediation(actions, rc, pack.remediation_context(state))
        if remediated:
            tracer().event("action_planner", "note", f"deterministic remediation applied to {remediated} "
                                                     f"(signature '{(rc.get('signature') or {}).get('id')}')")  # fmt: skip

        # 2) LLM narrative only for actions without a deterministic remediation; output constrained to the plan's types.
        open_actions = [a for a in actions if a["type"] not in remediated and a["type"] != "REPORT_ONLY"]
        llm = get_llm(cfg, "action_planner") if cfg.get("planner", {}).get("llm_narrative", True) else None
        out = None
        if open_actions and llm is not None:
            out = structured(
                llm,
                plan_text_model([a["type"] for a in open_actions]),
                "You are the action planner for an incident. The action TYPES are fixed; for each, write a specific "
                "description and concrete steps for a human, naming the exact error, resource and file from the facts. "
                "Never suggest deleting, dropping, truncating or overwriting data, files or resources.",
                f"Incident:\n{event_brief(event)}\n\nRoot cause: {rc['category']}: {rc['summary']}\n\n"
                f"Error digest:\n{rc.get('digest_text') or 'none'}\n\n"
                f"Actions (types fixed): {json.dumps([{'type': a['type'], 'params': a.get('params', {})} for a in open_actions])}\n\n"
                f"Runbook:\n{brain.runbooks(rc.get('runbooks', []), 3500)}",
                role="action_planner",
                purpose="action_narrative",
            )
        if out is not None:
            texts = {t.action_type: t for t in out.actions}
            for action in open_actions:
                text = texts.get(action["type"])
                if text:
                    action["description"] = text.description or action["description"]
                    action["steps"] = text.steps
            tracer().event("action_planner", "note", f"LLM narrative applied to {sorted(texts)}")
        elif open_actions:
            notes.append("action_planner: no LLM narrative; used playbook descriptions")
            tracer().event("action_planner", "note", f"playbook default descriptions for {[a['type'] for a in open_actions]}")

        root = pack.repo_root()
        if cfg["features"].get("generate_patches") and root and llm is not None:
            for action in actions:
                if action["type"] == "PROPOSE_CODE_CHANGE":
                    try:
                        patch = _draft_patch(llm, Path(root), state, action)
                        if patch and patch.unified_diff.strip():
                            action["patch"] = patch.unified_diff
                            action["patch_explanation"] = patch.explanation
                            tracer().event("action_planner", "note", f"patch drafted ({len(patch.unified_diff)} chars)")
                    except Exception as exc:
                        notes.append(f"action_planner: patch drafting failed ({exc})")
                        tracer().event("action_planner", "error", f"patch drafting failed: {exc}")
        return {"plan": actions, "notes": notes}

    return node


def route_after_planning(state: dict):
    return "code_review" if any(a["type"] == "PROPOSE_CODE_CHANGE" for a in state.get("plan", [])) else "review"
