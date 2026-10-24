import operator
from typing import Annotated, TypedDict


class IncidentState(TypedDict, total=False):
    incident_id: str
    event: dict  # normalized by the DomainPack
    force: bool  # replay: process even if already handled
    duplicate: bool
    no_incident_reason: str  # set by intake when the event is not an incident (e.g. job SUCCESS)
    severity: str
    evidence: Annotated[list, operator.add]  # gatherers run in parallel and append
    evidence_requests: list
    loops: int
    error_digest: dict  # log analyst: {"primary", "entries", "step", "text"}
    verifications: list  # verification tool calls already made
    verification: dict  # best verification result {"tool", "verdict", "summary", "evidence_id"}
    signature_matches: list
    root_cause: dict
    plan: list  # actions: {type, params, description, steps, patch?}
    code_review: dict
    decisions: list  # one per action, from the policy engine (+ LLM downgrade)
    executed: list
    status: str  # AUTO_RESOLVED | AWAITING_HUMAN | NO_INCIDENT | DUPLICATE
    report_paths: dict
    notes: Annotated[list, operator.add]  # warnings: LLM fallbacks, overrides, tool errors
