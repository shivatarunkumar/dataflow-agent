from domains.dataflow_ingest.event import normalize, payload_from_audit
from incident_core.brain import Brain
from incident_core.runner import cmd_brain_check
from incident_core.signatures import match_signatures

from tests.fakes import failed_quota_payload, rejected_header_payload

PIPELINE_CHECKS = ["contract_match", "file_name", "header", "delimiter", "trailer", "data_type"]


def top_match(brain, payload):
    event = normalize(payload, {})
    matches = match_signatures(brain.signatures, event["facts"], event["text"])
    return matches[0] if matches else None


def test_quota_failure_is_transient(cfg):
    m = top_match(Brain(cfg["brain_dir"]), failed_quota_payload())
    assert (m["id"], m["category"], m["runbook"]) == ("quota_exceeded", "TRANSIENT_INFRA", "QUOTA_EXCEEDED")


def test_rejected_header_is_data_quality(cfg):
    m = top_match(Brain(cfg["brain_dir"]), rejected_header_payload())
    assert (m["category"], m["runbook"]) == ("DATA_QUALITY", "header")


def test_exception_in_our_code_outranks_internal_error(cfg):
    payload = failed_quota_payload()
    payload["dataflow"]["error_messages"] = [{"text": (
        "An internal error occurred.\nTraceback (most recent call last):\n"
        '  File "/template/pipeline/transforms/parse.py", line 30, in process\n'
        "KeyError: 'contract'")}]  # fmt: skip
    assert top_match(Brain(cfg["brain_dir"]), payload)["id"] == "pipeline_code_exception"


def test_bare_traceback_is_only_a_weak_hint(cfg):
    """Every Dataflow failure has a traceback (Beam frames); on its own it must not decide CODE_BUG."""
    payload = failed_quota_payload()
    payload["dataflow"]["error_messages"] = [{"text": 'Traceback (most recent call last):\n  File "apache_beam/runners/common.py"'}]
    m = top_match(Brain(cfg["brain_dir"]), payload)
    assert m["id"] == "python_traceback" and m["confidence"] < 0.85


def test_quota_metric_name_alone_is_not_a_quota_error(cfg):
    payload = failed_quota_payload()
    payload["dataflow"]["error_messages"] = [{"text": "quota IN_USE_ADDRESSES in europe-west2: usage 0 / limit 4"}]
    m = top_match(Brain(cfg["brain_dir"]), payload)
    assert m is None or m["id"] != "quota_exceeded"


def test_key_errors_extracts_exception_line():
    from incident_core.signatures import key_errors

    found = key_errors([("gcp-2", 'Traceback (most recent call last):\\n  File "x.py"\\nUnicodeDecodeError: bad byte 0xe9')])
    assert found[0] == {"error": "UnicodeDecodeError: bad byte 0xe9", "source": "gcp-2"}


def test_rejected_never_matches_transient(cfg):
    payload = rejected_header_payload()
    payload["job"]["error_message"] = "QUOTA_EXCEEDED"  # misleading text must not matter for REJECTED
    assert top_match(Brain(cfg["brain_dir"]), payload)["category"] == "DATA_QUALITY"


def test_every_pipeline_check_has_a_runbook(cfg):
    keys = set(Brain(cfg["brain_dir"]).runbook_keys())
    assert set(PIPELINE_CHECKS) <= keys
    assert {"QUOTA_EXCEEDED", "DATAFLOW_JOB_FAILED", "RECON_MISMATCH", "UNKNOWN"} <= keys


def test_brain_check_passes(cfg):
    assert cmd_brain_check(cfg) == 0


def test_replay_payload_extracts_dataflow_job():
    row = {"job_id": "j1", "status": "FAILED", "error_message": "Dataflow job 2026-10-18_abc ended in JOB_STATE_FAILED. boom",
           "file_path": "gs://b/landing/x_20261015.csv", "source_bucket": "b", "final_file_path": "gs://b/invalid/2026/10/18/x_20261015.csv",
           "service_name": "workflows", "service_revision": "exec-9"}  # fmt: skip
    event = normalize(payload_from_audit(row, [], "p", "europe-west2", "dfa-ingest-file"), {})
    assert event["dataflow"]["job_id"] == "2026-10-18_abc"
    assert event["file"]["name"] == "landing/x_20261015.csv"
    assert event["orchestration"]["execution_id"] == "exec-9"
    assert event["subject"] == "x_20261015.csv"
