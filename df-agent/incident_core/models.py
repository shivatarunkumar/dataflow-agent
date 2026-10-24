"""Typed outputs of the LLM agents (validated with Pydantic) and shared constants.

Graph state itself stores plain dicts (model_dump) so checkpoints and reports stay simple JSON.
"""

from typing import Literal

from pydantic import BaseModel, Field

Category = Literal["TRANSIENT_INFRA", "DATA_QUALITY", "CODE_BUG", "CONFIG", "UNKNOWN"]
CATEGORIES = ["TRANSIENT_INFRA", "DATA_QUALITY", "CODE_BUG", "CONFIG", "UNKNOWN"]

AUTO_APPROVED, NEEDS_HUMAN, REPORT, REJECTED = "AUTO_APPROVED", "NEEDS_HUMAN", "REPORT", "REJECTED"


class RootCauseOut(BaseModel):
    """Investigator output."""

    category: Category = Field(description="Root-cause category.")
    summary: str = Field(description="2-4 sentences: what failed and why, citing evidence ids like audit-1.")
    evidence_refs: list[str] = Field(default_factory=list, description="Evidence ids supporting the conclusion.")
    confidence: float = Field(ge=0, le=1, description="0..1 confidence in the category.")
    needs_more_evidence: list[str] = Field(
        default_factory=list, description="Specific extra lookups needed, empty if the evidence is sufficient."
    )


def plan_text_model(action_types: list):
    """Planner output schema constrained to THIS plan's action types (the model can't invent e.g. 'CREATE')."""
    from pydantic import create_model

    allowed = Literal[tuple(action_types)]  # type: ignore[valid-type]
    action_text = create_model(
        "ActionText",
        action_type=(allowed, Field(description="One of the given action types, unchanged.")),
        description=(str, Field(description="What to do and why, specific to this incident, citing the error.")),
        steps=(list[str], Field(default_factory=list, description="Concrete steps for a human, in order.")),
    )
    return create_model("PlanTextOut", actions=(list[action_text], ...))


class PatchOut(BaseModel):
    unified_diff: str = Field(description="A git-style unified diff with a/ and b/ paths relative to the repo root.")
    explanation: str


class ActionReviewItem(BaseModel):
    action_type: str
    approve: bool = Field(description="False if the action is not justified by the evidence or looks risky.")
    concern: str = Field(default="", description="Why not, if approve is false.")


class ActionReviewOut(BaseModel):
    items: list[ActionReviewItem]


class CodeReviewOut(BaseModel):
    verdict: Literal["APPROVE", "REQUEST_CHANGES", "INSUFFICIENT"]
    comments: list[str] = Field(default_factory=list)
    risk: Literal["LOW", "MEDIUM", "HIGH"] = "MEDIUM"


class GathererSummaryOut(BaseModel):
    summary: str = Field(description="3-6 sentences summarising the most relevant findings, citing evidence ids.")


def _shrink(data, max_str: int, max_items: int = 50):
    """Keeps the structure; clips each long string head+tail (tracebacks end with the real error)."""
    from incident_core.errors import clip

    if isinstance(data, str):
        return clip(data, max_str)
    if isinstance(data, dict):
        return {k: _shrink(v, max_str, max_items) for k, v in data.items()}
    if isinstance(data, (list, tuple)):
        items = [_shrink(v, max_str, max_items) for v in list(data)[:max_items]]
        if len(data) > max_items:
            items.append(f"…{len(data) - max_items} more items")
        return items
    return data


def evidence(source: str, idx: int, kind: str, summary: str, data, max_chars: int = 6000) -> dict:
    return {"id": f"{source}-{idx}", "source": source, "kind": kind, "summary": summary,
            "data": _shrink(data, max_chars)}  # fmt: skip
