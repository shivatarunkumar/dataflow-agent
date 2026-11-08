"""Renders a ServiceNow Change Request draft (df-agent.md §21): change.md for people, change_request.json for the API.

The domain pack decides the content (DomainPack.change_request); this module only renders it. A CR is
  {"required": False, "reason": str, "instead": [str]}                        - no change needed
  {"required": True, "short_description": str,
   "fields": [{"label", "api", "value", "api_value"?}],                       - form fields, filled
   "manual_fields": [{"label", "api", "hint"}],                               - left for the requester
   "sections": [{"title", "api", "paragraphs": [], "steps": [], "bullets": [], "code": str}]}
`api` is the ServiceNow change_request column (None = shown in change.md only).
"""

import json
from datetime import datetime, timezone
from pathlib import Path

from jinja2 import Environment, FileSystemLoader

_ENV = Environment(loader=FileSystemLoader(Path(__file__).parent), trim_blocks=True, lstrip_blocks=True)


def section_text(section: dict) -> str:
    """Plain text of a section for a ServiceNow text field (numbered steps, dashed bullets)."""
    parts = list(section.get("paragraphs") or [])
    parts += [f"{i}. {step}" for i, step in enumerate(section.get("steps") or [], 1)]
    parts += [f"- {bullet}" for bullet in section.get("bullets") or []]
    if section.get("code"):
        parts.append(section["code"])
    return "\n".join(parts)


def servicenow_payload(cr: dict, incident_id: str) -> dict:
    """Body for POST /api/now/table/change_request (string values, ServiceNow column names)."""
    payload = {"short_description": cr["short_description"]}
    for f in cr.get("fields", []):
        value = f.get("api_value", f.get("value"))  # e.g. Risk "Moderate" is stored as "3"
        if f.get("api") and value not in (None, ""):
            payload[f["api"]] = str(value)
    for section in cr.get("sections", []):
        if section.get("api"):
            payload[section["api"]] = section_text(section)
    payload["correlation_id"] = incident_id  # links the CR back to the df-agent incident
    return payload


def write_change(cr: dict | None, state: dict, md_path: Path, json_path: Path) -> dict:
    """Writes change.md (always) and change_request.json (only when a CR is needed). Returns a summary for the report."""
    incident_id = state["incident_id"]
    cr = cr or {"required": False, "reason": "This incident type does not define change requests.", "instead": []}
    context = {"cr": cr, "incident_id": incident_id, "event": state["event"], "rc": state.get("root_cause") or {},
               "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"), "section_text": section_text}  # fmt: skip
    Path(md_path).write_text(_ENV.get_template("change.md.j2").render(**context))
    if cr.get("required"):
        Path(json_path).write_text(json.dumps(servicenow_payload(cr, incident_id), indent=2, default=str))
    elif Path(json_path).exists():
        Path(json_path).unlink()  # a stale draft from an earlier run of this incident
    fields = {f.get("api"): f.get("value") for f in cr.get("fields", [])}
    return {"required": bool(cr.get("required")), "reason": cr.get("reason", ""),
            "short_description": cr.get("short_description", ""), "type": fields.get("type"),
            "risk": fields.get("risk"), "priority": fields.get("priority")}  # fmt: skip
