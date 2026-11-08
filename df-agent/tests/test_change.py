"""Incident folder layout and the ServiceNow change request draft (df-agent.md §21). No LLM, no GCP."""

import json
from pathlib import Path

from incident_core.report.writer import detail_file
from incident_core.trace import start_trace, end_trace

from tests.fakes import (PERMISSION_TRACEBACK, WORKFLOW_FAILED, FakePack, airflow_launch_failed_payload,
                         failed_quota_payload, rejected_header_payload)  # fmt: skip
from tests.test_graph import UNICODE_TRACEBACK, run


def folder(result) -> Path:
    return Path(result["report_paths"]["report_pdf"]).parent


def change_md(result) -> str:
    return Path(result["report_paths"]["change_md"]).read_text()


def change_json(result) -> dict:
    return json.loads(Path(result["report_paths"]["change_json"]).read_text())


def permission_incident(cfg):
    payload = failed_quota_payload("job-iam")
    payload["dataflow"]["error_messages"] = [{"text": PERMISSION_TRACEBACK[:1400]}]
    pack = FakePack(cfg)
    pack.error_texts = [PERMISSION_TRACEBACK] * 4 + [WORKFLOW_FAILED]
    return run(cfg, pack, payload)[0]


def test_only_report_and_change_at_the_top(cfg):
    result, _ = run(cfg, FakePack(cfg), failed_quota_payload("job-layout"))
    assert {p.name for p in folder(result).iterdir()} == {"report.pdf", "change.md", "details"}
    details = {p.name for p in (folder(result) / "details").iterdir()}
    assert {"report.md", "evidence.json", "state.json", "review.md", "trace.md"} <= details


def test_automatic_retry_needs_no_change(cfg):
    result, _ = run(cfg, FakePack(cfg), failed_quota_payload("job-auto"))
    text = change_md(result)
    assert text.startswith("# Change Request: not required") and "re-run automatically" in text
    assert not Path(result["report_paths"]["change_json"]).exists()


def test_rejected_file_needs_no_change(cfg):
    result, _ = run(cfg, FakePack(cfg), rejected_header_payload("job-hdr"))
    text = change_md(result)
    assert "not required" in text and "data owner" in text and "landing/user_20261016.csv" in text


def test_iam_fix_is_a_normal_change_with_full_plan(cfg):
    result = permission_incident(cfg)
    text = change_md(result)
    assert text.startswith("# Change Request (draft): Config change for customer_load")
    for section in ("Change details", "Fill in when raising the CR", "Description", "Justification", "Implementation plan",
                    "Risk and impact analysis", "Test plan", "Backout plan", "Post-implementation verification",
                    "Communication plan", "Approvals", "Related records", "Closure"):  # fmt: skip
        assert f"## {section}" in text, section
    assert "| Type | Normal |" in text and "| Risk | Moderate |" in text
    assert f"IAM bindings of {FakePack.WORKER_SA}" in text
    assert "CONFIRMED" in text  # verification is part of the justification
    # Review the Terraform plan before applying; the retry comes once, at the end.
    plan = text.split("## Implementation plan")[1].split("## ")[0]
    assert plan.index("make plan") < plan.index("make gcs-setup") < plan.index("make agent-approve INCIDENT=job-iam")
    assert plan.count("make agent-approve") == 1

    payload = change_json(result)
    assert payload["type"] == "normal" and payload["risk"] == "3" and payload["priority"] == "3"
    assert payload["correlation_id"] == "job-iam"
    for column in ("short_description", "description", "justification", "implementation_plan", "risk_impact_analysis",
                   "test_plan", "backout_plan", "cmdb_ci", "assignment_group", "business_service"):  # fmt: skip
        assert payload.get(column), column


def test_pending_retry_alone_is_a_standard_change(cfg):
    pack = FakePack(cfg, facts={"retries_24h": 2})
    result, _ = run(cfg, pack, failed_quota_payload("job-quota-3"))
    text = change_md(result)
    assert "| Type | Standard |" in text and "| Risk | Low |" in text
    assert "Pre-approved standard change" in text and "CAB date" not in text
    assert change_json(result)["type"] == "standard"


def test_code_bug_change_has_image_backout(cfg):
    payload = failed_quota_payload("job-unicode-cr")
    payload["file"]["name"] = "landing/customer_20261021.csv"
    payload["dataflow"]["error_messages"] = [{"text": UNICODE_TRACEBACK}]
    pack = FakePack(cfg)
    pack.error_text = UNICODE_TRACEBACK
    result, _ = run(cfg, pack, payload)
    text = change_md(result)
    assert "Code fix in Dataflow pipeline for customer_load: UnicodeDecodeError" in text
    assert "Dataflow pipeline image gcs-to-bq" in text and "make docker" in text and "make test" in text
    assert "git revert the fix" in text.split("## Backout plan")[1]


def test_launch_parameter_fix_skips_terraform(cfg):
    result, _ = run(cfg, FakePack(cfg), airflow_launch_failed_payload("job-launch-cr"))
    text = change_md(result)
    plan = text.split("## Implementation plan")[1].split("## ")[0]
    assert "make plan" not in plan and "make dags-deploy" in plan
    assert "| Failing layer | AIRFLOW |" in text


def test_report_links_the_change(cfg):
    result = permission_incident(cfg)
    report = Path(result["report_paths"]["report_md"]).read_text()
    assert "## Change request" in report and "change.md" in report


def test_old_flat_layout_is_tidied_into_history(tmp_path):
    incident = tmp_path / "actions" / "job-old"
    incident.mkdir(parents=True)
    for name in ("report.pdf", "report.md", "evidence.json", "trace.jsonl", "trace.20261103T182342.jsonl"):
        (incident / name).write_text("x")
    (incident / "llm").mkdir()
    start_trace("job-old", str(incident / "details"))
    end_trace()
    assert {p.name for p in incident.iterdir()} == {"report.pdf", "details"}
    moved = next((incident / "details" / "history").glob("flat-layout.*"))
    assert {"report.md", "evidence.json", "trace.jsonl", "llm"} <= {p.name for p in moved.iterdir()}


def test_detail_file_falls_back_to_the_flat_layout(tmp_path):
    cfg = {"actions_dir": str(tmp_path)}
    (tmp_path / "old").mkdir()
    (tmp_path / "old" / "state.json").write_text("{}")
    assert detail_file(cfg, "old", "state.json") == tmp_path / "old" / "state.json"
    assert detail_file(cfg, "new", "state.json") == tmp_path / "new" / "details" / "state.json"
