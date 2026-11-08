"""Pure decision logic shared by the DAGs (no Airflow / GCP imports, so it is unit-tested locally)."""

import re
from datetime import datetime

from dfa_common import config

# Airflow's default allowed_run_id_pattern is ^[A-Za-z0-9_.~:+-]+$ (no slashes); run_id is at most 250 chars.
_RUN_ID_UNSAFE = re.compile(r"[^A-Za-z0-9_.~:+-]")

# Task order in gcs_to_bq_dataflow: the first failed one is the root of an Airflow-side failure.
INGEST_TASK_ORDER = ["register_job", "is_new_file", config.DATAFLOW_TASK_ID, "finalize"]


def ingest_run_id(name: str, generation: str) -> str:
    relative = name[len(config.LANDING_PREFIX):] if name.startswith(config.LANDING_PREFIX) else name
    return f"file__{generation}__{_RUN_ID_UNSAFE.sub('_', relative)}"[:250]


def file_event(message: dict, attributes: dict, message_id: str, delivery_attempt) -> dict | None:
    """One GCS notification -> trigger kwargs {trigger_run_id, conf}, or None for events we ignore."""
    if attributes.get("eventType") != "OBJECT_FINALIZE":
        return None
    name = message["name"]
    if name.endswith("/") or not name.startswith(config.LANDING_PREFIX):
        return None
    generation = str(message.get("generation"))
    return {
        "trigger_run_id": ingest_run_id(name, generation),
        "conf": {
            "bucket": message["bucket"],
            "name": name,
            "generation": generation,
            "size": int(message.get("size", 0)),
            "time_created": message.get("timeCreated"),
            "message_id": message_id,
            "subscription": config.SUBSCRIPTION,
            "delivery_attempt": delivery_attempt or None,
        },
    }


def decide_outcome(dataflow_job: dict, status: str | None) -> tuple:
    """(final_status, error_code) from the Dataflow job ({} if none was created) and the status Dataflow wrote.

    error_code is None when the status Dataflow wrote stands (SUCCESS / REJECTED / its own FAILED).
    """
    state = dataflow_job.get("state") or "NOT_STARTED"
    if state == "JOB_STATE_DONE" and status not in (None, "IN_PROGRESS"):
        return status, None
    if not dataflow_job:
        return "FAILED", "AIRFLOW_TASK_FAILED"  # e.g. the flex template launch was rejected
    return "FAILED", "DATAFLOW_JOB_FAILED"


def failure_message(dataflow_job: dict, dataflow_task_state: str | None, airflow_error: str) -> str:
    """Keeps the 'Dataflow job <id> ended in <state>.' prefix: replays recover the Dataflow job id from it."""
    last_line = next((line.strip() for line in reversed(airflow_error.splitlines()) if line.strip()), "")
    message = (f"Dataflow job {dataflow_job.get('id', '')} ended in {dataflow_job.get('state') or 'NOT_STARTED'}. "
               f"Airflow task {config.DATAFLOW_TASK_ID} {dataflow_task_state or 'not run'}.")  # fmt: skip
    return f"{message} {last_line}"[:900].strip()


def first_failed_task(task_states: dict, exclude: tuple = ()) -> str:
    """task_states: task_id -> state. The earliest failed task in DAG order ('' if none)."""
    for task_id in INGEST_TASK_ORDER:
        if task_id not in exclude and task_states.get(task_id) == "failed":
            return task_id
    return ""


def archive_destination(name: str, status: str, now: datetime) -> str:
    prefix = config.ARCHIVE_PREFIX if status == "SUCCESS" else config.INVALID_PREFIX
    relative = name[len(config.LANDING_PREFIX):] if name.startswith(config.LANDING_PREFIX) else name
    return f"{prefix}{now:%Y/%m/%d}/{relative}"
