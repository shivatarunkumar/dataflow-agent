"""Error event (schema v2, project.md 7a): a self-contained description of a FAILED/REJECTED job for the df-agent.

build_error_event() is pure (unit-tested); publish_error_event() sends it to DFA_ERROR_TOPIC.
"""

import json
from datetime import datetime, timezone
from urllib.parse import quote

from dfa_common import config

SCHEMA_VERSION = "2"
TASK_ERROR_CHARS = 2500

RETRY_HINTS = {
    "REJECTED": ("File failed validation and was moved to invalid/. Fix the issues in failed_checks and re-upload "
                 "the corrected file to landing/."),
    "FAILED": ("Processing failed (see orchestration.task_error / dataflow.error_messages). After fixing the cause, "
               "re-upload the same file to landing/ to retry; the original is in final_path."),
}  # fmt: skip


def airflow_run_url(airflow_uri: str, dag_id: str, run_id: str) -> str:
    if not airflow_uri:
        return ""
    return f"{airflow_uri.rstrip('/')}/dags/{dag_id}/grid?dag_run_id={quote(run_id, safe='')}"


def dataflow_console_url(dataflow_job_id: str) -> str:
    if not dataflow_job_id:
        return ""
    return (f"https://console.cloud.google.com/dataflow/jobs/{config.REGION}/{dataflow_job_id}"
            f"?project={config.PROJECT_ID}")  # fmt: skip


def build_error_event(*, job: dict, status: str, file: dict, failed_checks: list, dataflow: dict,
                      orchestration: dict) -> dict:
    """job: job_metadata columns; file: bucket/name/generation/original_path/final_path;
    dataflow: job_id/state/launch_error/error_messages; orchestration: Airflow fields (see project.md 7a)."""
    job_id = job.get("job_id") or ""
    dataset = f"{config.PROJECT_ID}.{config.AUDIT_DATASET}"
    return {
        "schema_version": SCHEMA_VERSION,
        "event_type": f"ingestion.job.{status.lower()}",
        "published_at": datetime.now(timezone.utc).isoformat(),
        "job": {**job, "job_id": job_id, "status": status},
        "file": file,
        "failed_checks": failed_checks,
        "dataflow": {**dataflow, "console_url": dataflow_console_url(dataflow.get("job_id", ""))},
        "orchestration": {"type": "airflow", **orchestration},
        "audit_queries": {
            "job_metadata": f"SELECT * FROM `{dataset}.job_metadata` WHERE job_id = '{job_id}'",
            "validation_metadata": (f"SELECT * FROM `{dataset}.validation_metadata` WHERE job_id = '{job_id}' "
                                    "ORDER BY check_name"),
        },  # fmt: skip
        "retry_hint": RETRY_HINTS.get(status, RETRY_HINTS["FAILED"]),
    }


def event_attributes(event: dict) -> dict:
    job = event["job"]
    return {
        "status": event["job"]["status"],
        "error_code": job.get("error_code") or "NONE",
        "job_id": job.get("job_id") or "",
        "job_name": job.get("job_name") or "unknown",
        "file_name": event["file"].get("name") or "",
        "orchestrator": "airflow",
    }


def publish_error_event(event: dict) -> str:
    """Publishes the event; returns the Pub/Sub message id."""
    from airflow.providers.google.cloud.hooks.pubsub import PubSubHook

    data = json.dumps(event, default=str).encode("utf-8")
    ids = PubSubHook().publish(
        project_id=config.PROJECT_ID, topic=config.ERROR_TOPIC,
        messages=[{"data": data, "attributes": event_attributes(event)}],
    )  # fmt: skip
    return ids[0] if ids else ""


def task_log_tail(dag_id: str, run_id: str, task_id: str, try_number: int, map_index: int = -1) -> str:
    """Tail of an Airflow task log from the Composer environment bucket ('' if it can't be read)."""
    if not config.COMPOSER_BUCKET or not try_number:
        return ""
    from airflow.providers.google.cloud.hooks.gcs import GCSHook

    mapped = f"map_index={map_index}/" if map_index is not None and map_index >= 0 else ""
    name = f"logs/dag_id={dag_id}/run_id={run_id}/task_id={task_id}/{mapped}attempt={try_number}.log"
    try:
        text = GCSHook().download(bucket_name=config.COMPOSER_BUCKET, object_name=name).decode("utf-8", "replace")
    except Exception:
        return ""
    return text[-TASK_ERROR_CHARS:]
