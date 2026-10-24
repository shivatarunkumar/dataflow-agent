"""DomainPack: everything incident-type specific. The graph, agents, policy engine and reports are generic."""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Callable


@dataclass
class ToolSpec:
    """A read-only evidence tool. `func(**kwargs) -> (summary: str, data)`."""

    name: str
    description: str
    func: Callable
    args_schema: dict = field(default_factory=dict)  # {"arg": "description"} for the LLM
    # True if the output describes THIS incident's failure (errors, logs, check results). Only signal evidence is
    # matched by root-cause signatures; context (quota readings, history, object state) never is.
    signal: bool = False


@dataclass
class Gatherer:
    """An evidence-gathering agent (e.g. audit, gcp): its tools and the default calls it always makes."""

    name: str  # graph node name, e.g. "audit_agent"
    source: str  # evidence id prefix, e.g. "audit"
    role_description: str
    tools: list
    default_calls: Callable  # (event: dict) -> list[tuple[tool_name, kwargs]]


class DomainPack(ABC):
    name: str = "domain"

    @abstractmethod
    def parse_event(self, payload: dict, attributes: dict) -> dict:
        """Raw message -> normalized event dict. Must contain: incident_id, status, title, facts (for signatures),
        text (corpus for signatures), links, raw."""

    @abstractmethod
    def event_from_reference(self, reference: str) -> dict:
        """Rebuilds a normalized event without the queue (e.g. from a job_id) for `agent-replay`."""

    @abstractmethod
    def gatherers(self) -> list:
        """Evidence gatherers (list[Gatherer])."""

    def incident_check(self, event: dict) -> tuple:
        """(is_incident, reason). Non-incidents (e.g. a successful job) are closed without investigation."""
        return True, ""

    @abstractmethod
    def severity(self, event: dict) -> str:
        """e.g. SEV2 / SEV3."""

    @abstractmethod
    def build_actions(self, category: str, event: dict, playbook: list) -> list:
        """Concrete actions for the category: [{type, params, description}] (params filled by code, not the LLM)."""

    @abstractmethod
    def policy_facts(self, action: dict, event: dict, root_cause: dict) -> dict:
        """Facts the policy engine evaluates for this action (computed live)."""

    @abstractmethod
    def execute(self, action: dict, incident_id: str) -> dict:
        """Executes an approved executable action; returns a result record. Must be non-destructive."""

    @abstractmethod
    def config_values(self) -> dict:
        """Values policy param_rules may reference (e.g. landing_bucket)."""

    @abstractmethod
    def report_sections(self, state: dict) -> dict:
        """Domain-specific report content: {"facts": [(label, value)], "failed_checks": [...], "links": {...},
        "timeline": [...]}."""

    def repo_root(self):
        """Repository root for read-only code access (code proposals / git apply --check). None = disabled."""
        return None

    def verification_calls(self, signature: dict, digest: dict, event: dict, evidence: list) -> list:
        """Read-only tool calls that CONFIRM or REFUTE the top signature's hypothesis, as [(tool_name, kwargs)].
        A verification tool returns data with "verdict": CONFIRMED | REFUTED | INCONCLUSIVE."""
        return []

    def remediation_context(self, state: dict) -> dict:
        """Values for {placeholders} in signature remediation templates (brain/signatures.yaml)."""
        return {}

    def all_tools(self) -> dict:
        """Every tool by name (gatherer tools + verification-only tools)."""
        return {t.name: t for g in self.gatherers() for t in g.tools}
