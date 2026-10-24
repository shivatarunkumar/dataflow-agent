"""Error event (schema v1, published by workflows/ingest_file.yaml publish_failure) -> normalized incident event."""

import re


def _base(path: str) -> str:
    return (path or "").rsplit("/", 1)[-1]


def normalize(payload: dict, attributes: dict | None = None) -> dict:
    attributes = attributes or {}
    job = payload.get("job") or {}
    file = payload.get("file") or {}
    dataflow = payload.get("dataflow") or {}
    orchestration = payload.get("orchestration") or {}
    checks = payload.get("failed_checks") or []

    job_id = job.get("job_id") or attributes.get("job_id")
    if not job_id:
        raise ValueError("event has no job.job_id")
    status = job.get("status") or attributes.get("status")
    file_name = _base(file.get("name") or attributes.get("file_name", ""))
    error_texts = [m.get("text", "") for m in dataflow.get("error_messages") or []]

    text = "\n".join(filter(None, [
        job.get("status_reason"), job.get("error_message"), dataflow.get("launch_error"), dataflow.get("state"),
        *error_texts, *[f"{c.get('check_name')}: {c.get('summary')}" for c in checks],
    ]))  # fmt: skip

    return {
        "incident_id": job_id,
        "status": status,
        "subject": file_name,
        "title": f"{status} {job.get('job_name') or ''} {file_name}".strip(),
        "facts": {
            "status": status,
            "error_code": job.get("error_code"),
            "failed_checks": [c.get("check_name") for c in checks],
            "dataflow_state": dataflow.get("state"),
        },
        "runbook_hints": [c.get("check_name") for c in checks],
        "text": text,
        "links": {
            "dataflow": dataflow.get("console_url"),
            "workflow_execution": orchestration.get("console_url"),
        },
        "job": job,
        "file": file,
        "dataflow": dataflow,
        "orchestration": orchestration,
        "failed_checks": checks,
        "raw": payload,
    }


_DATAFLOW_JOB = re.compile(r"Dataflow job (\S*) ended in (\S+?)\.")


def payload_from_audit(job_row: dict, failed_checks: list, project: str, region: str, workflow: str) -> dict:
    """Rebuilds a v1-shaped payload from job_metadata (+ failed validation rows) for replays."""
    match = _DATAFLOW_JOB.search(job_row.get("error_message") or "")
    dataflow_job_id = match.group(1) if match else ""
    dataflow_state = match.group(2) if match else ("JOB_STATE_DONE" if job_row.get("status") == "REJECTED" else "")
    execution_id = job_row.get("service_revision") if job_row.get("service_name") == "workflows" else None
    return {
        "schema_version": "1",
        "job": {k: job_row.get(k) for k in (
            "job_id", "status", "status_reason", "error_code", "error_message", "job_name", "contract_name",
            "target_table", "rows_in_file", "rows_loaded", "validation_status", "recon_status", "start_time",
            "end_time", "attempt_number")},  # fmt: skip
        "file": {
            "bucket": job_row.get("source_bucket"),
            "name": (job_row.get("file_path") or "").split("/", 3)[-1] if job_row.get("file_path") else None,
            "generation": job_row.get("file_generation"),
            "original_path": job_row.get("file_path"),
            "final_path": job_row.get("final_file_path"),
        },
        "failed_checks": failed_checks,
        "dataflow": {
            "job_id": dataflow_job_id,
            "state": dataflow_state,
            "launch_error": "",
            "error_messages": [],
            "console_url": f"https://console.cloud.google.com/dataflow/jobs/{region}/{dataflow_job_id}?project={project}" if dataflow_job_id else "",
        },
        "orchestration": {"workflow": workflow, "execution_id": execution_id},
    }
