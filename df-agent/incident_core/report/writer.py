"""Writes actions/<incident_id>/ (df-agent.md §21):

  report.pdf, change.md                    what a person opens
  details/  report.md, change_request.json, evidence.json, review.md, state.json, trace.md, proposed.patch
"""

import json
from datetime import datetime, timezone
from pathlib import Path

from jinja2 import Environment, FileSystemLoader

from incident_core.llm import llm_label
from incident_core.models import NEEDS_HUMAN, REPORT
from incident_core.report.change import write_change
from incident_core.report.pdf import markdown_to_pdf
from incident_core.trace import render_markdown, tracer

_ENV = Environment(loader=FileSystemLoader(Path(__file__).parent), trim_blocks=True, lstrip_blocks=True)

DETAILS = "details"


def incident_dir(cfg: dict, incident_id: str) -> Path:
    path = Path(cfg["actions_dir"]) / incident_id
    path.mkdir(parents=True, exist_ok=True)
    return path


def details_dir(cfg: dict, incident_id: str) -> Path:
    path = incident_dir(cfg, incident_id) / DETAILS
    path.mkdir(parents=True, exist_ok=True)
    return path


def detail_file(cfg: dict, incident_id: str, name: str) -> Path:
    """A supporting file of an incident: details/<name>, or <name> for incidents written before §21."""
    folder = Path(cfg["actions_dir"]) / incident_id
    new = folder / DETAILS / name
    return new if new.exists() or not (folder / name).exists() else folder / name


def write_report(cfg: dict, pack, state: dict) -> dict:
    out = incident_dir(cfg, state["incident_id"])
    details = details_dir(cfg, state["incident_id"])
    actions = [dict(a, decision=d) for a, d in zip(state.get("plan", []), state.get("decisions", []))]
    human = [a for a in actions if a["decision"]["decision"] in (NEEDS_HUMAN, REPORT) and a["type"] != "REPORT_ONLY"]
    context = {
        "incident_id": state["incident_id"],
        "event": state["event"],
        "severity": state.get("severity", "-"),
        "status": state.get("status", "-"),
        "rc": state["root_cause"],
        "actions": actions,
        "human_actions": human,
        "executed": state.get("executed", []),
        "code_reviews": (state.get("code_review") or {}).get("reviews", []),
        "evidence": state.get("evidence", []),
        "sections": pack.report_sections(state),
        "notes": state.get("notes", []),
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        "llm_label": llm_label(cfg, "investigator"),
    }
    records = tracer().records()
    tools = sum(1 for r in records if r["kind"] == "tool")
    elapsed = records[-1]["elapsed_s"] if records else 0
    context["trace_stats"] = (f"{len(records)} steps in {elapsed}s: {tracer().llm_calls} LLM call(s), "
                              f"{tools} tool call(s).")  # fmt: skip
    context["trace_table"] = render_markdown(records)
    (details / "trace.md").write_text(f"# Investigation trace: {state['incident_id']}\n\n{context['trace_stats']}\n\n"
                                      f"{context['trace_table']}\n")  # fmt: skip

    paths = {"report_pdf": out / "report.pdf", "change_md": out / "change.md", "report_md": details / "report.md",
             "change_json": details / "change_request.json", "evidence": details / "evidence.json",
             "review": details / "review.md", "state": details / "state.json"}  # fmt: skip

    patches = [a["patch"] for a in actions if a.get("patch")]
    if patches:
        paths["patch"] = details / "proposed.patch"
        paths["patch"].write_text("\n".join(patches))

    # The change request goes first: the report links to it.
    context["change"] = write_change(pack.change_request(state), state, paths["change_md"], paths["change_json"])

    markdown = _ENV.get_template("report.md.j2").render(**context)
    paths["report_md"].write_text(markdown)
    markdown_to_pdf(markdown, str(paths["report_pdf"]), title=f"Incident {state['incident_id']}")
    paths["evidence"].write_text(json.dumps(state.get("evidence", []), indent=2, default=str))

    review = ["# Review decisions", ""]
    for a in actions:
        d = a["decision"]
        review.append(f"- **{a['type']}** → {d['decision']}: {'; '.join(d['reasons'])}")
        if d.get("llm_review"):
            review.append(f"  - LLM reviewer: approve={d['llm_review']['approve']} {d['llm_review']['concern']}")
    for r in context["code_reviews"]:
        review.append(f"- Code review: {r['verdict']} (risk {r['risk']}): {'; '.join(r['comments'])}")
    paths["review"].write_text("\n".join(review) + "\n")

    # Snapshot used by `agent-approve` (actions + decisions + event).
    snapshot = {k: state.get(k) for k in ("incident_id", "event", "severity", "root_cause", "plan", "decisions", "executed", "status")}
    paths["state"].write_text(json.dumps(snapshot, indent=2, default=str))
    return {k: str(v) for k, v in paths.items()}
