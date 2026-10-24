"""LangGraph incident workflow (see df-agent.md §4).

intake ─┬─ (already handled) ─────────────────────────────────────────────────────────────▶ END
        ├─ (not an incident, e.g. job SUCCESS) → close_no_incident ───────────▶ report → record
        └─ [gatherers in parallel] → log_analyst → investigator ⇄ (more evidence, ≤ N rounds) → planner →
           (code_review) → action_review → executor → report → record

Every node runs inside a traced span (incident_core.trace): start, end, duration and a one-line outcome.
"""

import logging
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from langgraph.graph import END, START, StateGraph

from incident_core.agents.gatherer import make_gatherer_node
from incident_core.agents.investigator import make_investigator_node, route_after_investigation
from incident_core.agents.log_analyst import make_log_analyst_node
from incident_core.agents.planner import make_planner_node, route_after_planning
from incident_core.agents.reviewers import make_action_review_node, make_code_review_node
from incident_core.brain import Brain
from incident_core.llm import llm_label
from incident_core.models import AUTO_APPROVED, NEEDS_HUMAN, REPORT
from incident_core.report.writer import incident_dir, write_report
from incident_core.state import IncidentState
from incident_core.trace import tracer

log = logging.getLogger(__name__)
ROLES = ("audit_agent", "gcp_agent", "investigator", "action_planner", "code_reviewer", "action_reviewer")  # LLM roles
NO_INCIDENT = "NO_INCIDENT"


def _outcome(out: dict) -> str:
    """One-line summary of what a node produced, for the trace."""
    parts = []
    if out.get("evidence") is not None:
        parts.append(f"{len(out['evidence'])} evidence item(s)")
    if out.get("error_digest") is not None:
        primary = out["error_digest"].get("primary")
        parts.append(f"primary error {primary['kind'].upper()}" if primary else "no error lines found")
    if out.get("verification"):
        parts.append(f"verification {out['verification']['verdict']}")
    if out.get("root_cause"):
        rc = out["root_cause"]
        parts.append(f"root cause {rc['category']} ({rc['confidence']:.2f})")
    if out.get("plan") is not None:
        parts.append(f"plan {[a['type'] for a in out['plan']]}")
    if out.get("decisions") is not None:
        parts.append("decisions " + ", ".join(f"{d['action_type']}={d['decision']}" for d in out["decisions"]))
    if out.get("executed") is not None:
        parts.append(f"executed {[(e['action_type'], e['status']) for e in out['executed']] or 'nothing'}")
    if out.get("status"):
        parts.append(f"status {out['status']}")
    if out.get("report_paths"):
        parts.append(f"report {out['report_paths'].get('report_pdf')}")
    return "; ".join(parts) or "done"


def traced(name: str, fn):
    def wrapper(state: dict) -> dict:
        with tracer().span(name, f"{name} started") as span:
            out = fn(state) or {}
            span["summary"] = _outcome(out)
        return out

    return wrapper


def build_graph(cfg: dict, pack, store, checkpointer=None):
    brain = Brain(cfg["brain_dir"])
    gatherers = pack.gatherers()
    gatherer_names = [g.name for g in gatherers]

    def intake(state: dict) -> dict:
        incident_id, event = state["incident_id"], state["event"]
        tracer().event("intake", "note", f"event: {event.get('title')} (status {event.get('status')})")
        if not state.get("force") and store.is_handled(incident_id):
            tracer().event("intake", "decision", "already handled earlier - skipping (use agent-replay to force)")
            return {"duplicate": True, "status": "DUPLICATE"}
        incident_dir(cfg, incident_id)
        is_incident, reason = pack.incident_check(event)
        if not is_incident:
            tracer().event("intake", "decision", f"not an incident: {reason}")
            return {"duplicate": False, "severity": "NONE", "loops": 0, "no_incident_reason": reason}
        severity = pack.severity(event)
        tracer().event("intake", "decision", f"incident confirmed, severity {severity}; starting investigation")
        return {"duplicate": False, "severity": severity, "loops": 0}

    def route_intake(state: dict):
        if state.get("duplicate"):
            return END
        if state.get("no_incident_reason"):
            return "close_no_incident"
        return gatherer_names  # fan-out: gatherers run in parallel; the investigator waits for all of them

    def close_no_incident(state: dict) -> dict:
        reason = state["no_incident_reason"]
        return {
            "root_cause": {"category": NO_INCIDENT, "confidence": 1.0, "summary": reason, "method": "status_check",
                           "evidence_refs": [], "runbooks": [], "signature": None},  # fmt: skip
            "plan": [{"type": "REPORT_ONLY", "params": {}, "description": f"No action needed. {reason}", "steps": []}],
            "decisions": [{"action_type": "REPORT_ONLY", "decision": REPORT, "reasons": ["not an incident"]}],
            "executed": [],
            "status": NO_INCIDENT,
        }

    def executor(state: dict) -> dict:
        executed = []
        for action, decision in zip(state.get("plan", []), state.get("decisions", [])):
            if decision["decision"] != AUTO_APPROVED:
                continue
            record = {"action_type": action["type"], "params": action.get("params", {}), "actor": "df-agent",
                      "at": datetime.now(timezone.utc).isoformat()}  # fmt: skip
            try:
                record.update(status="DONE", result=pack.execute(action, state["incident_id"]))
                tracer().event("executor", "execution", f"{action['type']} DONE: {record['result']}", result=record["result"])
            except Exception as exc:
                record.update(status="FAILED", result={"error": str(exc)})
                tracer().event("executor", "execution", f"{action['type']} FAILED: {exc}", level=logging.ERROR)
            executed.append(record)
        if not executed:
            tracer().event("executor", "note", "nothing auto-approved; no actions executed")

        waiting = any(d["decision"] == NEEDS_HUMAN for d in state.get("decisions", []))
        manual = any(
            d["decision"] == REPORT and a["type"] != "REPORT_ONLY"
            for a, d in zip(state.get("plan", []), state.get("decisions", []))
        )
        failed = any(e["status"] != "DONE" for e in executed)
        status = "AWAITING_HUMAN" if (waiting or manual or failed or not executed) else "AUTO_RESOLVED"
        if not state.get("plan"):
            status = "AWAITING_HUMAN"
        return {"executed": executed, "status": status}

    def report(state: dict) -> dict:
        return {"report_paths": write_report(cfg, pack, state)}

    def record(state: dict) -> dict:
        executed = state.get("executed", [])
        rc = state.get("root_cause", {})
        store.record({
            "incident_id": state["incident_id"],
            "subject": state["event"].get("subject"),
            "event_status": state["event"].get("status"),
            "status": state["status"],
            "severity": state.get("severity"),
            "category": rc.get("category"),
            "confidence": rc.get("confidence"),
            "root_cause": rc.get("summary"),
            "actions": state.get("plan", []),
            "decisions": state.get("decisions", []),
            "executed_actions": executed,
            "executed_retry_count": sum(1 for e in executed if e["action_type"] == "RETRY_FILE" and e["status"] == "DONE"),
            "report_path": (state.get("report_paths") or {}).get("report_pdf"),
            "llm_models": {role: llm_label(cfg, role) for role in ROLES},
            "actor": "df-agent",
        })  # fmt: skip
        tracer().event("record", "note", f"recorded status {state['status']} in agent_incidents")
        return {}

    nodes = {
        "intake": intake,
        "close_no_incident": close_no_incident,
        **{g.name: make_gatherer_node(g, cfg) for g in gatherers},
        "log_analyst": make_log_analyst_node(cfg, brain, pack),
        "investigator": make_investigator_node(cfg, brain),
        "action_planner": make_planner_node(cfg, brain, pack),
        "code_reviewer": make_code_review_node(cfg, pack),
        "action_reviewer": make_action_review_node(cfg, brain, pack),
        "executor": executor,
        "report_writer": report,
        "record": record,
    }
    g = StateGraph(IncidentState)
    for name, fn in nodes.items():
        g.add_node(name, traced(name, fn))

    g.add_edge(START, "intake")
    # path_map lists each router's possible targets so get_graph() draws the real branches (make agent-graph)
    g.add_conditional_edges("intake", route_intake, [END, "close_no_incident", *gatherer_names])
    g.add_edge("close_no_incident", "report_writer")
    g.add_edge(gatherer_names, "log_analyst")  # waits for all gatherers
    g.add_edge("log_analyst", "investigator")
    g.add_conditional_edges(
        "investigator",
        lambda s: gatherer_names if route_after_investigation(s) == "more_evidence" else "action_planner",
        [*gatherer_names, "action_planner"],
    )
    g.add_conditional_edges(
        "action_planner",
        lambda s: "code_reviewer" if route_after_planning(s) == "code_review" else "action_reviewer",
        ["code_reviewer", "action_reviewer"],
    )
    g.add_edge("code_reviewer", "action_reviewer")
    g.add_edge("action_reviewer", "executor")
    g.add_edge("executor", "report_writer")
    g.add_edge("report_writer", "record")
    g.add_edge("record", END)
    return g.compile(checkpointer=checkpointer)


def sqlite_checkpointer(cfg: dict):
    from langgraph.checkpoint.sqlite import SqliteSaver

    path = Path(cfg["state_dir"]) / "checkpoints.sqlite"
    path.parent.mkdir(parents=True, exist_ok=True)
    return SqliteSaver(sqlite3.connect(str(path), check_same_thread=False))
