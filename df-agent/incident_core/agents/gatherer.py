"""Generic evidence-gathering agent (used for the audit agent and the GCP agent).

1. Always runs the domain's default lookups (deterministic: reliable even with a small model).
2. Lets the LLM choose up to N extra read-only lookups (tool calling), guided by investigator requests.
3. Asks the LLM for a short summary. Every step degrades gracefully if the LLM is absent or fails.
Every tool call and LLM call is traced (incident_core.trace).
"""

import json
import logging
import time

from langchain_core.messages import HumanMessage, SystemMessage, ToolMessage

from incident_core.agents.common import event_brief, evidence_text
from incident_core.domain import Gatherer
from incident_core.llm import get_llm, model_name, structured
from incident_core.models import GathererSummaryOut, evidence
from incident_core.trace import tracer


def _tool_schema(spec) -> dict:
    props, required = {}, []
    for arg, meta in spec.args_schema.items():
        props[arg] = {"type": meta.get("type", "string"), "description": meta.get("description", "")}
        if meta.get("required", True):
            required.append(arg)
    return {
        "type": "function",
        "function": {
            "name": spec.name,
            "description": spec.description,
            "parameters": {"type": "object", "properties": props, "required": required},
        },
    }


def _fmt_args(kwargs: dict) -> str:
    return ", ".join(f"{k}={str(v)[:40]}" for k, v in kwargs.items())


def make_gatherer_node(gatherer: Gatherer, cfg: dict):
    tools = {t.name: t for t in gatherer.tools}
    limits = cfg["limits"]

    def run_tool(name: str, kwargs: dict, idx: int, chosen_by: str) -> dict:
        spec = tools.get(name)
        t0 = time.monotonic()
        if spec is None:
            tracer().event(gatherer.name, "tool", f"{chosen_by} asked for unknown tool {name}", level=logging.WARNING)
            return evidence(gatherer.source, idx, "error", f"unknown tool {name}", {"args": kwargs})
        try:
            summary, data = spec.func(**kwargs)
            item = evidence(gatherer.source, idx, name, summary, data, limits["evidence_item_max_chars"])
            item["signal"] = spec.signal
            kind = "signal" if spec.signal else "context"
            tracer().event(gatherer.name, "tool", f"{name}({_fmt_args(kwargs)}) [{chosen_by}, {kind}] → {item['id']}: {summary[:160]}",
                           time.monotonic() - t0, tool=name, args=kwargs, evidence_id=item["id"])  # fmt: skip
            return item
        except Exception as exc:  # tool failures are evidence too (e.g. permission problems)
            tracer().event(gatherer.name, "tool", f"{name}({_fmt_args(kwargs)}) [{chosen_by}] FAILED: {exc}",
                           time.monotonic() - t0, level=logging.WARNING, tool=name, args=kwargs)  # fmt: skip
            return evidence(gatherer.source, idx, "error", f"{name}({kwargs}) failed: {exc}", {})

    def node(state: dict) -> dict:
        event = state["event"]
        prior = [e for e in state.get("evidence", []) if e["source"] == gatherer.source]
        first_round = not prior
        items, done = [], set()

        def add(name, kwargs, chosen_by):
            key = (name, json.dumps(kwargs, sort_keys=True, default=str))
            if key in done:
                tracer().event(gatherer.name, "tool", f"skipped duplicate call {name}({_fmt_args(kwargs)})")
                return None
            done.add(key)
            item = run_tool(name, kwargs, len(prior) + len(items) + 1, chosen_by)
            items.append(item)
            return item

        if first_round:
            for name, kwargs in gatherer.default_calls(event):
                add(name, kwargs, "default")

        settings = cfg.get("gatherers", {})
        llm = get_llm(cfg, gatherer.name)
        requests = state.get("evidence_requests") or []
        notes = []
        budget = limits["gatherer_extra_tool_calls"]
        # "on_request": the LLM picks extra tools only for specific investigator requests (default; the default
        # lookups already cover the standard evidence). "always": also on the first round. "never": deterministic only.
        mode = settings.get("llm_tool_selection", "on_request")
        use_llm_tools = llm is not None and budget > 0 and (
            (mode == "always" and (requests or first_round)) or (mode == "on_request" and requests))  # fmt: skip
        if use_llm_tools:
            if requests:
                tracer().event(gatherer.name, "note", f"investigator requested: {requests}")
            try:
                bound = llm.bind_tools([_tool_schema(t) for t in gatherer.tools])
                messages = [
                    SystemMessage(content=(
                        f"You are the {gatherer.name}. {gatherer.role_description} "
                        f"You may call at most {budget} more read-only tools to fill gaps. "
                        "If the evidence below is already sufficient, call no tools."
                    )),
                    HumanMessage(content=(
                        f"Incident:\n{event_brief(event)}\n\nRequests from the investigator: {requests or 'none'}\n\n"
                        f"Evidence so far:\n{evidence_text(prior + items, 4000, include_data=False)}"
                    )),
                ]  # fmt: skip
                calls_made = 0
                while calls_made < budget:
                    t0 = time.monotonic()
                    response = bound.invoke(messages)
                    tool_calls = getattr(response, "tool_calls", None) or []
                    chosen = [f"{c['name']}({_fmt_args(c.get('args') or {})})" for c in tool_calls]
                    tracer().llm_call(gatherer.name, "choose_tools", model_name(llm), str(messages[0].content),
                                      str(messages[-1].content)[:4000], {"tool_calls": chosen}, None,
                                      time.monotonic() - t0, 1)  # fmt: skip
                    if not tool_calls:
                        tracer().event(gatherer.name, "note", "LLM chose no extra lookups (evidence sufficient)")
                        break
                    messages.append(response)
                    for call in tool_calls:
                        if calls_made >= budget:
                            break
                        calls_made += 1
                        item = add(call["name"], call.get("args") or {}, "llm")
                        content = "duplicate call skipped" if item is None else f"{item['summary']}\n{json.dumps(item['data'], default=str)[:2000]}"
                        messages.append(ToolMessage(content=content, tool_call_id=call.get("id") or call["name"]))
            except Exception as exc:
                notes.append(f"{gatherer.name}: LLM tool loop failed ({exc}); used default lookups only")
                tracer().event(gatherer.name, "error", f"LLM tool loop failed: {exc}", level=logging.WARNING)

        summary = None if not settings.get("llm_summary", False) else structured(
            llm,
            GathererSummaryOut,
            f"You are the {gatherer.name}. Summarise evidence for an incident investigator. Be factual; cite ids.",
            f"Incident:\n{event_brief(event)}\n\nEvidence:\n{evidence_text(prior + items, limits['prompt_evidence_max_chars'])}",
            role=gatherer.name,
            purpose="summarise_evidence",
        )
        if summary is not None:
            items.append(evidence(gatherer.source, len(prior) + len(items) + 1, "summary", summary.summary, {}))
            tracer().event(gatherer.name, "note", f"summary: {summary.summary[:200]}")
        elif llm is not None and settings.get("llm_summary", False):
            notes.append(f"{gatherer.name}: LLM summary unavailable")
        return {"evidence": items, "notes": notes}

    return node
