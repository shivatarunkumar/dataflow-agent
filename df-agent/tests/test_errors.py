"""Error digest: the deterministic extraction the whole investigation now rests on."""

import json

from incident_core.errors import build_digest, clip
from incident_core.models import evidence

from tests.fakes import PERMISSION_TRACEBACK, QUOTA_ERROR, WORKFLOW_FAILED

UNICODE = ("Traceback (most recent call last):\n  File \"apache_beam/io/textio.py\", line 252, in read_records\n"
           "UnicodeDecodeError: 'utf-8' codec can't decode byte 0xe9 in position 6: invalid continuation byte")  # fmt: skip


def test_permission_error_at_bottom_of_traceback():
    d = build_digest([("gcp-2", [{"text": PERMISSION_TRACEBACK}] * 4), ("gcp-3", WORKFLOW_FAILED)])
    p = d["primary"]
    assert p["kind"] == "permission"
    assert (p["http_code"], p["status"], p["reason"]) == (403, "PERMISSION_DENIED", "accessDenied")
    assert p["permission"] == "bigquery.tables.create"
    assert p["resource"] == "dataset bigquerytarun:raw"
    assert p["exception_chain"] == ["RuntimeError", "HttpForbiddenError"]
    assert p["message"].startswith("Access Denied: Dataset bigquerytarun:raw")
    assert p["count"] == 4 and len(d["entries"]) == 1  # retries grouped; "Workflow failed" is only a wrapper
    assert d["step"].startswith("LoadToTargetTable/WriteToBigQuery")
    assert "http=403" in d["text"] and "permission=bigquery.tables.create" in d["text"]


def test_found_in_json_escaped_log_payload():
    """Cloud Logging payloads are JSON-encoded; newlines/quotes arrive escaped."""
    escaped = json.dumps({"message": PERMISSION_TRACEBACK})
    assert build_digest([("gcp-4", escaped)])["primary"]["permission"] == "bigquery.tables.create"


def test_survives_evidence_clipping():
    """v1 kept only the head of long messages and lost the error. Clipping now keeps the tail."""
    long_text = "x" * 20000 + "\n" + PERMISSION_TRACEBACK
    item = evidence("gcp", 2, "get_dataflow_errors", "s", [{"text": long_text}], max_chars=6000)
    assert build_digest([(item["id"], item["data"])])["primary"]["kind"] == "permission"
    assert clip("a" * 100 + "TAIL", 50).endswith("TAIL")


def test_encoding_and_quota_kinds():
    assert build_digest([("e", UNICODE)])["primary"]["kind"] == "encoding"
    assert build_digest([("e", QUOTA_ERROR)])["primary"]["kind"] == "quota"


def test_most_specific_error_is_primary():
    d = build_digest([("a", UNICODE), ("b", PERMISSION_TRACEBACK)])
    assert d["primary"]["kind"] == "permission"


def test_no_error_text():
    d = build_digest([("event", "Dataflow job 123 ended in JOB_STATE_FAILED. "), ("audit-1", {"job_id": "x"})])
    assert d["primary"] is None and d["text"] == "no error found in signal evidence"
