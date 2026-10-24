"""End-to-end graph scenarios in rules-only mode (no LLM, no GCP)."""

from pathlib import Path

from incident_core.graph import build_graph
from incident_core.models import AUTO_APPROVED, NEEDS_HUMAN, REPORT

from tests.fakes import FakePack, failed_quota_payload, local_store, rejected_header_payload


def run(cfg, pack, payload, force=False):
    store = local_store(cfg)
    pack.attach_store(store)
    event = pack.event(payload)
    graph = build_graph(cfg, pack, store)
    return graph.invoke({"incident_id": event["incident_id"], "event": event, "force": force}), store


def test_quota_failure_is_retried_automatically(cfg):
    pack = FakePack(cfg)
    result, store = run(cfg, pack, failed_quota_payload())

    assert result["root_cause"]["category"] == "TRANSIENT_INFRA"
    decisions = {d["action_type"]: d["decision"] for d in result["decisions"]}
    assert decisions["RETRY_FILE"] == AUTO_APPROVED
    assert result["status"] == "AUTO_RESOLVED"
    assert pack.executed[0]["params"] == {"bucket": "test-landing", "source": "invalid/2026/10/18/customer_20261015.csv",
                                          "destination": "landing/customer_20261015.csv"}  # fmt: skip
    assert store.retries_24h("customer_20261015.csv") == 1
    for key in ("report_md", "report_pdf", "evidence", "review", "state"):
        assert Path(result["report_paths"][key]).exists()


def test_rejected_header_produces_report_only(cfg):
    pack = FakePack(cfg)
    result, _ = run(cfg, pack, rejected_header_payload())

    assert result["root_cause"]["category"] == "DATA_QUALITY"
    assert [a["type"] for a in result["plan"]] == ["PROPOSE_DATA_FIX"]
    assert result["decisions"][0]["decision"] == REPORT
    assert result["status"] == "AWAITING_HUMAN"
    assert pack.executed == []
    report = Path(result["report_paths"]["report_md"]).read_text()
    assert "header" in report and "PROPOSE_DATA_FIX" in report


def test_retry_limit_hands_over_to_a_human(cfg):
    pack = FakePack(cfg, facts={"retries_24h": 2})
    result, _ = run(cfg, pack, failed_quota_payload("job-quota-2"))

    assert {d["action_type"]: d["decision"] for d in result["decisions"]}["RETRY_FILE"] == NEEDS_HUMAN
    assert result["status"] == "AWAITING_HUMAN"
    assert pack.executed == []
    assert "make agent-approve INCIDENT=job-quota-2" in Path(result["report_paths"]["report_md"]).read_text()


UNICODE_TRACEBACK = (
    "Error message from worker: Traceback (most recent call last):\n"
    '  File "/usr/local/lib/python3.12/site-packages/apache_beam/io/textio.py", line 252, in read_records\n'
    "    yield self._coder.decode(record)\n"
    "UnicodeDecodeError: 'utf-8' codec can't decode byte 0xe9 in position 6: invalid continuation byte"
)


def test_code_bug_is_not_misread_as_quota(cfg):
    """Regression: a quota *reading* (context) must not make every FAILED job look like QUOTA_EXCEEDED."""
    payload = failed_quota_payload("job-unicode")
    payload["file"]["name"] = "landing/customer_20261021.csv"
    payload["dataflow"]["error_messages"] = [{"time": "2026-10-18T19:21:50Z", "text": UNICODE_TRACEBACK}]
    pack = FakePack(cfg)
    pack.error_text = UNICODE_TRACEBACK
    result, _ = run(cfg, pack, payload)

    rc = result["root_cause"]
    assert rc["category"] == "CODE_BUG"
    assert rc["signature"]["id"] == "encoding_error"
    assert all(m["id"] != "quota_exceeded" for m in result["signature_matches"])
    assert rc["primary_error"]["kind"] == "encoding"
    assert "UnicodeDecodeError" in rc["primary_error"]["exception_chain"]
    assert [a["type"] for a in result["plan"]] == ["PROPOSE_CODE_CHANGE", "ESCALATE"]
    assert pack.executed == []  # never auto-retried
    # The quota reading was still collected, as context.
    assert any(e["kind"] == "get_quota" and not e["signal"] for e in result["evidence"])


def test_permission_denied_is_config_verified_and_not_data_quality(cfg):
    """Regression for incident ba0fbdb5: a 403 at the bottom of a Beam traceback was called DATA_QUALITY."""
    from tests.fakes import PERMISSION_TRACEBACK, WORKFLOW_FAILED

    payload = failed_quota_payload("job-iam")
    payload["dataflow"]["error_messages"] = [{"text": PERMISSION_TRACEBACK[:1400]}]  # the event only has the head
    pack = FakePack(cfg)
    pack.error_texts = [PERMISSION_TRACEBACK] * 4 + [WORKFLOW_FAILED]
    result, _ = run(cfg, pack, payload)

    primary = result["error_digest"]["primary"]
    assert primary["kind"] == "permission"
    assert primary["http_code"] == 403 and primary["status"] == "PERMISSION_DENIED"
    assert primary["permission"] == "bigquery.tables.create"
    assert "bigquerytarun:raw" in primary["resource"]
    assert "HttpForbiddenError" in primary["exception_chain"]
    assert primary["count"] >= 4  # bundle retries grouped
    assert "LoadToTargetTable/WriteToBigQuery" in result["error_digest"]["step"]

    # Hypothesis verified against (fake) live IAM, using the job's worker SA and the dataset from the error.
    assert pack.verified == [(FakePack.WORKER_SA, "bigquery.tables.create", "raw")]
    rc = result["root_cause"]
    assert rc["category"] == "CONFIG"
    assert rc["signature"]["id"] == "permission_denied"
    assert rc["confidence"] >= 0.97 and rc["verification"]["verdict"] == "CONFIRMED"

    plan = {a["type"]: a for a in result["plan"]}
    assert set(plan) == {"PROPOSE_CONFIG_CHANGE", "RETRY_FILE"}
    fix = plan["PROPOSE_CONFIG_CHANGE"]
    assert "bigquery.tables.create" in fix["description"] and FakePack.WORKER_SA in fix["description"]
    assert any("roles/bigquery.dataEditor" in s for s in fix["steps"])
    decisions = {d["action_type"]: d["decision"] for d in result["decisions"]}
    assert decisions["RETRY_FILE"] == NEEDS_HUMAN  # retry only after a human fixes IAM
    assert pack.executed == []
    report = Path(result["report_paths"]["report_md"]).read_text()
    assert "What failed" in report and "bigquery.tables.create" in report and "CONFIRMED" in report


def test_refuted_verification_does_not_decide(cfg):
    from tests.fakes import PERMISSION_TRACEBACK

    payload = failed_quota_payload("job-iam-2")
    payload["dataflow"]["error_messages"] = []
    pack = FakePack(cfg, permission_verdict="REFUTED")
    pack.error_texts = [PERMISSION_TRACEBACK]
    result, _ = run(cfg, pack, payload)
    rc = result["root_cause"]
    assert rc["confidence"] <= 0.6  # IAM says the permission IS granted: not decisive, needs a human
    assert result["status"] == "AWAITING_HUMAN"


def test_llm_data_quality_without_failed_checks_is_rejected(cfg, monkeypatch):
    """Guard: a model claiming DATA_QUALITY for a FAILED job with no failed checks is overruled."""
    from incident_core.agents import investigator as inv
    from incident_core.models import RootCauseOut

    monkeypatch.setattr(inv, "get_llm", lambda cfg, role: object())
    monkeypatch.setattr(inv, "structured", lambda *a, **k: RootCauseOut(
        category="DATA_QUALITY", summary="The data is incomplete and contains errors", evidence_refs=["gcp-2"], confidence=0.8))
    payload = failed_quota_payload("job-guard")
    payload["dataflow"]["error_messages"] = []
    pack = FakePack(cfg)
    pack.error_texts = ["something odd happened"]  # no signature applies, so the LLM's answer is evaluated
    result, _ = run(cfg, pack, payload)
    assert result["root_cause"]["category"] != "DATA_QUALITY"
    assert any("DATA_QUALITY rejected" in n for n in result["notes"])


def test_successful_job_is_not_an_incident(cfg):
    payload = rejected_header_payload("job-ok")
    payload["job"].update(status="SUCCESS", error_code=None, rows_in_file=20, rows_loaded=20, validation_status="PASSED",
                          recon_status="PASSED", target_table="bigquerytarun.raw.user")  # fmt: skip
    payload["failed_checks"] = []
    payload["file"]["final_path"] = "gs://test-landing/archive/2026/10/18/user_20261016.csv"
    pack = FakePack(cfg)
    result, store = run(cfg, pack, payload, force=True)

    assert result["status"] == "NO_INCIDENT"
    assert result["root_cause"]["category"] == "NO_INCIDENT"
    assert "evidence" not in result or result["evidence"] == []  # no investigation, no LLM
    assert pack.executed == []
    report = Path(result["report_paths"]["report_md"]).read_text()
    assert "Not an incident" in report and "completed successfully" in report
    assert store.is_handled("job-ok")


def test_handled_incident_is_skipped_unless_forced(cfg):
    pack = FakePack(cfg)
    run(cfg, pack, failed_quota_payload("job-dup"))
    second, _ = run(cfg, pack, failed_quota_payload("job-dup"))
    assert second["status"] == "DUPLICATE"
    assert len(pack.executed) == 1

    forced, _ = run(cfg, pack, failed_quota_payload("job-dup"), force=True)
    assert forced["status"] in ("AUTO_RESOLVED", "AWAITING_HUMAN")
