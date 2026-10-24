import pytest

from incident_core.brain import Brain
from incident_core.models import AUTO_APPROVED, NEEDS_HUMAN, REJECTED, REPORT
from incident_core.policy import PolicyEngine

GOOD_FACTS = {"event_status": "FAILED", "category": "TRANSIENT_INFRA", "confidence": 0.9, "retries_24h": 0,
              "source_exists": True, "destination_exists": False, "stuck_ingest_jobs": 0}  # fmt: skip


@pytest.fixture
def engine(cfg):
    return PolicyEngine(Brain(cfg["brain_dir"]).policies, {"landing_bucket": "test-landing"})


def retry(**params):
    base = {"bucket": "test-landing", "source": "invalid/2026/10/18/c.csv", "destination": "landing/c.csv"}
    return {"type": "RETRY_FILE", "params": {**base, **params}, "description": "retry"}


def test_transient_retry_is_auto_approved(engine):
    assert engine.evaluate(retry(), GOOD_FACTS)["decision"] == AUTO_APPROVED


@pytest.mark.parametrize("fact, value", [
    ("event_status", "REJECTED"), ("category", "DATA_QUALITY"), ("confidence", 0.5),
    ("retries_24h", 2), ("stuck_ingest_jobs", 1),
])  # fmt: skip
def test_auto_rule_failures_need_a_human(engine, fact, value):
    decision = engine.evaluate(retry(), {**GOOD_FACTS, fact: value})
    assert decision["decision"] == NEEDS_HUMAN
    assert fact in decision["reasons"][0]


@pytest.mark.parametrize("facts", [
    {**GOOD_FACTS, "retries_24h": 5}, {**GOOD_FACTS, "source_exists": False}, {**GOOD_FACTS, "destination_exists": True},
])  # fmt: skip
def test_not_approvable_even_by_human(engine, facts):
    assert engine.evaluate(retry(), facts)["decision"] == REJECTED


@pytest.mark.parametrize("params", [
    {"bucket": "other-bucket"},                          # outside the landing bucket
    {"source": "landing/c.csv"},                         # source must be invalid/
    {"destination": "archive/c.csv"},                    # destination must be landing/
    {"destination": "landing/../archive/c.csv"},         # traversal
    {"source": None},                                    # missing (file was never moved)
])  # fmt: skip
def test_bad_parameters_are_rejected(engine, params):
    assert engine.evaluate(retry(**params), GOOD_FACTS)["decision"] == REJECTED


@pytest.mark.parametrize("action", [
    {"type": "DELETE_FILE", "params": {"path": "landing/c.csv"}},
    {"type": "RUN_SQL", "params": {"sql": "TRUNCATE TABLE raw.customer"}},
    {"type": "RETRY_FILE", "params": {"bucket": "test-landing", "source": "invalid/c.csv", "destination": "landing/c.csv",
                                      "then": "rm the original"}},
    {"type": "PROPOSE_DATA_FIX", "params": {"cmd": "gsutil rm gs://test-landing/invalid/c.csv"}},
    {"type": "PROPOSE_CONFIG_CHANGE", "params": {}, "manual_command": "terraform destroy -auto-approve"},
])  # fmt: skip
def test_destructive_or_unknown_actions_are_rejected(engine, action):
    assert engine.evaluate(action, GOOD_FACTS)["decision"] == REJECTED


def test_prose_mentioning_delete_does_not_block_a_retry(engine):
    action = retry()
    action["description"] = "Copy the file back. This does not delete or overwrite the original."
    assert engine.evaluate(action, GOOD_FACTS)["decision"] == AUTO_APPROVED


@pytest.mark.parametrize("action_type", ["PROPOSE_DATA_FIX", "PROPOSE_CODE_CHANGE", "PROPOSE_CONFIG_CHANGE", "ESCALATE", "REPORT_ONLY"])
def test_non_executable_actions_are_report_only(engine, action_type):
    assert engine.evaluate({"type": action_type, "params": {}}, {})["decision"] == REPORT


def test_human_recheck(engine):
    assert engine.recheck_for_human(retry(), {**GOOD_FACTS, "confidence": 0.1}) == []
    assert engine.recheck_for_human(retry(), {**GOOD_FACTS, "destination_exists": True})
    assert engine.recheck_for_human({"type": "PROPOSE_CODE_CHANGE", "params": {}}, GOOD_FACTS)
