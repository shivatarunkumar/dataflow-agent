"""Audit agent tools: fixed, parameterised, read-only BigQuery queries. Each returns (summary, data)."""

import json

from incident_core.domain import ToolSpec


def _rows(clients, sql: str, **params) -> list:
    from google.cloud import bigquery

    query_params = [
        bigquery.ScalarQueryParameter(k, "INT64" if isinstance(v, int) else "STRING", v) for k, v in params.items()
    ]
    job = clients.bigquery.query(sql, job_config=bigquery.QueryJobConfig(query_parameters=query_params))
    return [json.loads(json.dumps(dict(row.items()), default=str)) for row in job.result()]


def build_audit_tools(clients, gcp: dict) -> list:
    ds = f"`{gcp['project']}.{gcp['audit_dataset']}"
    jobs, checks, incidents = f"{ds}.job_metadata`", f"{ds}.validation_metadata`", f"{ds}.{gcp['incidents_table']}`"

    def get_job(job_id: str):
        rows = _rows(clients, f"SELECT * FROM {jobs} WHERE job_id = @job_id", job_id=job_id)
        if not rows:
            return f"No job_metadata row for {job_id}", {}
        r = rows[0]
        return (f"job {job_id}: status={r.get('status')} error_code={r.get('error_code')} rows_in_file={r.get('rows_in_file')} "
                f"rows_loaded={r.get('rows_loaded')} validation={r.get('validation_status')} recon={r.get('recon_status')} "
                f"reason={r.get('status_reason')}"), r  # fmt: skip

    def get_validation_results(job_id: str):
        rows = _rows(clients, f"""
            SELECT check_name, check_type, stage, validation_result, summary, records_evaluated, records_failed,
                   failed_checks, SUBSTR(TO_JSON_STRING(details), 1, 3000) AS details
            FROM {checks} WHERE job_id = @job_id ORDER BY stage DESC, check_name""", job_id=job_id)
        failed = [f"{r['check_name']} ({r['summary']})" for r in rows if r["validation_result"] == "FAIL"]
        return f"{len(rows)} checks for {job_id}; failed: {failed or 'none'}", rows

    def get_file_history(file_name: str):
        rows = _rows(clients, f"""
            SELECT job_id, status, error_code, file_generation, attempt_number, final_file_path, created_at
            FROM {jobs} WHERE file_name = @file_name ORDER BY created_at DESC LIMIT 20""", file_name=file_name)
        statuses = [r["status"] for r in rows]
        return f"{len(rows)} jobs for file {file_name} (newest first): {statuses}", rows

    def get_recent_failures(job_name: str, hours: int = 24):
        hours = max(1, min(int(hours), 168))
        rows = _rows(clients, f"""
            SELECT status, error_code, COUNT(*) AS jobs FROM {jobs}
            WHERE job_name = @job_name AND created_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @hours HOUR)
            GROUP BY status, error_code ORDER BY jobs DESC""", job_name=job_name, hours=hours)
        return f"{job_name} outcomes in the last {hours}h: {[(r['status'], r['error_code'], r['jobs']) for r in rows]}", rows

    def get_agent_history(file_name: str):
        rows = _rows(clients, f"""
            SELECT incident_id, status, category, executed_retry_count, actor, created_at FROM {incidents}
            WHERE subject = @subject ORDER BY created_at DESC LIMIT 20""", subject=file_name)
        retries = sum(int(r.get("executed_retry_count") or 0) for r in rows)
        return f"{len(rows)} previous agent records for {file_name}; retries executed: {retries}", rows

    s = lambda d: {"type": "string", "description": d}  # noqa: E731
    return [
        # signal=True: describes THIS job's failure (its error message / its check results).
        ToolSpec("get_job", "job_metadata row for a job_id.", get_job, {"job_id": s("job id (UUID)")}, signal=True),
        ToolSpec("get_validation_results", "All validation_metadata check results for a job_id.", get_validation_results,
                 {"job_id": s("job id (UUID)")}, signal=True),
        # Context only: other jobs / past incidents must never decide this incident's root cause.
        ToolSpec("get_file_history", "Previous jobs for the same file name (attempts, generations, statuses).",
                 get_file_history, {"file_name": s("file name, e.g. customer_20261015.csv")}),
        ToolSpec("get_recent_failures", "Outcome counts for a job_name (e.g. customer_load) in the last N hours.",
                 get_recent_failures, {"job_name": s("job name, e.g. customer_load"),
                                       "hours": {"type": "integer", "description": "1-168", "required": False}}),
        ToolSpec("get_agent_history", "Previous df-agent incidents/retries for the same file name.", get_agent_history,
                 {"file_name": s("file name")}),
    ]  # fmt: skip


def default_audit_calls(event: dict) -> list:
    calls = [("get_job", {"job_id": event["incident_id"]}),
             ("get_validation_results", {"job_id": event["incident_id"]})]  # fmt: skip
    if event.get("subject"):
        calls += [("get_file_history", {"file_name": event["subject"]}), ("get_agent_history", {"file_name": event["subject"]})]
    if event["job"].get("job_name"):
        calls.append(("get_recent_failures", {"job_name": event["job"]["job_name"], "hours": 24}))
    return calls
