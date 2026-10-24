"""Deterministic action policy (brain/policies.yaml). Runs before, and cannot be overridden by, any LLM.

Decisions: AUTO_APPROVED | NEEDS_HUMAN | REPORT | REJECTED (see incident_core.models).
"""

import json
import re

from incident_core.models import AUTO_APPROVED, NEEDS_HUMAN, REJECTED, REPORT

_OPS = {
    "equals": lambda a, b: a == b,
    "gte": lambda a, b: a is not None and a >= b,
    "gt": lambda a, b: a is not None and a > b,
    "lte": lambda a, b: a is not None and a <= b,
    "lt": lambda a, b: a is not None and a < b,
    "in": lambda a, b: a in b,
}


def check_conditions(conditions: list, facts: dict) -> list:
    """Returns human-readable failures; empty list means every condition holds."""
    failures = []
    for cond in conditions or []:
        name = cond["fact"]
        op = next(k for k in cond if k != "fact")
        if name not in facts:
            failures.append(f"{name}: unknown")
        elif not _OPS[op](facts[name], cond[op]):
            failures.append(f"{name}={facts[name]!r} (needs {op} {cond[op]!r})")
    return failures


class PolicyEngine:
    def __init__(self, policies: dict, config_values: dict):
        self.policies = policies
        self.config_values = config_values
        self.forbidden = [re.compile(p, re.IGNORECASE) for p in policies.get("forbidden_patterns", [])]

    def is_executable(self, action_type: str) -> bool:
        return bool(self.policies["action_types"].get(action_type, {}).get("executable"))

    def _forbidden_hits(self, action: dict) -> list:
        # Only what can be executed: parameters and any shell command offered to a human. LLM prose such as
        # "this does not delete the original" must not reject a valid action.
        blob = json.dumps(action.get("params", {})) + "\n" + action.get("manual_command", "")
        return [p.pattern for p in self.forbidden if p.search(blob)]

    def _param_violations(self, action: dict) -> list:
        rules = self.policies.get("param_rules", {}).get(action["type"])
        if not rules:
            return []
        params, violations = action.get("params", {}), []
        bucket_key = rules.get("bucket_must_equal")
        if bucket_key and params.get("bucket") != self.config_values.get(bucket_key):
            violations.append(f"bucket {params.get('bucket')!r} is not the configured {bucket_key}")
        for field, prefix_key in (("source", "source_prefix"), ("destination", "destination_prefix")):
            prefix = rules.get(prefix_key)
            value = params.get(field) or ""
            if prefix and (not value.startswith(prefix) or ".." in value):
                violations.append(f"{field} {value!r} must start with {prefix!r}")
        return violations

    def evaluate(self, action: dict, facts: dict) -> dict:
        action_type = action.get("type")
        decision = {"action_type": action_type, "decision": None, "reasons": [], "policy_facts": facts}

        if action_type not in self.policies["action_types"]:
            decision.update(decision=REJECTED, reasons=[f"unknown action type {action_type!r}"])
            return decision
        hits = self._forbidden_hits(action)
        if hits:
            decision.update(decision=REJECTED, reasons=[f"forbidden pattern {h}" for h in hits])
            return decision
        if not self.is_executable(action_type):
            decision.update(decision=REPORT, reasons=["manual action for a human; never executed by the agent"])
            return decision
        violations = self._param_violations(action)
        if violations:
            decision.update(decision=REJECTED, reasons=violations)
            return decision

        auto_failures = check_conditions(self.policies.get("auto_approve", {}).get(action_type), facts)
        if not auto_failures:
            decision.update(decision=AUTO_APPROVED, reasons=["all auto-approve conditions hold"])
            return decision
        human_failures = check_conditions(self.policies.get("human_approvable", {}).get(action_type), facts)
        if human_failures:
            decision.update(decision=REJECTED, reasons=["not approvable even by a human: " + "; ".join(human_failures)])
        else:
            decision.update(decision=NEEDS_HUMAN, reasons=["auto-approve blocked: " + "; ".join(auto_failures)])
        return decision

    def recheck_for_human(self, action: dict, facts: dict) -> list:
        """Used by `agent-approve` with fresh facts. Returns blocking reasons (empty = may execute)."""
        if not self.is_executable(action.get("type")):
            return ["not an executable action type"]
        problems = self._forbidden_hits(action) + self._param_violations(action)
        return problems + check_conditions(self.policies.get("human_approvable", {}).get(action["type"]), facts)
