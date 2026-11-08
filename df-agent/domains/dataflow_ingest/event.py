"""Error event -> normalized incident event.

Schema v2 is published by the Airflow DAG gcs_to_bq_dataflow (finalize / on_ingest_failure, dags/dfa_common/events.py).
Schema v1 (Cloud Workflows, before 2026-11) is still accepted so old incidents can be replayed.
"""

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
    airflow = orchestration.get("type") == "airflow"

    # An Airflow failure before register_job finished has no job_id; the DAG run identifies the incident then.
    job_id = job.get("job_id") or attributes.get("job_id") or (orchestration.get("dag_run_id") if airflow else None)
    if not job_id:
        raise ValueError("event has no job.job_id")
    status = job.get("status") or attributes.get("status")
    file_name = _base(file.get("name") or attributes.get("file_name", ""))
    error_texts = [m.get("text", "") for m in dataflow.get("error_messages") or []]

    text = "\n".join(filter(None, [
        job.get("status_reason"), job.get("error_message"), dataflow.get("launch_error"), dataflow.get("state"),
        orchestration.get("task_error"), *error_texts, *[f"{c.get('check_name')}: {c.get('summary')}" for c in checks],
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
            "orchestrator": "airflow" if airflow else "workflows",
            "failed_task": orchestration.get("failed_task") or "",
        },
        "runbook_hints": [c.get("check_name") for c in checks],
        "text": text,
        "links": {
            "airflow_dag_run" if airflow else "workflow_execution": orchestration.get("console_url"),
            "dataflow": dataflow.get("console_url"),
        },
        "job": {**job, "job_id": job.get("job_id") or ""},
        "file": file,
        "dataflow": dataflow,
        "orchestration": orchestration,
        "failed_checks": checks,
        "raw": payload,
    }


_DATAFLOW_JOB = re.compile(r"Dataflow job (\S*) ended in (\S+?)\.")


def payload_from_audit(job_row: dict, failed_checks: list, project: str, region: str, composer_env: str) -> dict:
    """Rebuilds an event payload from job_metadata (+ failed validation rows) for replays.

    job_metadata.service_name says who orchestrated the job: 'composer' (service_revision = Airflow dag_run_id) or
    'workflows' (service_revision = workflow execution id, schema v1).
    """
    match = _DATAFLOW_JOB.search(job_row.get("error_message") or "")
    dataflow_job_id = match.group(1) if match else ""
    dataflow_state = match.group(2) if match else ("JOB_STATE_DONE" if job_row.get("status") == "REJECTED" else "")
    if job_row.get("service_name") == "composer":
        schema, orchestration = "2", {"type": "airflow", "environment": composer_env, "dag_id": "gcs_to_bq_dataflow",
                                      "dag_run_id": job_row.get("service_revision")}  # fmt: skip
    else:
        execution_id = job_row.get("service_revision") if job_row.get("service_name") == "workflows" else None
        schema, orchestration = "1", {"workflow": "dfa-ingest-file", "execution_id": execution_id}
    return {
        "schema_version": schema,
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
        "orchestration": orchestration,
    }
