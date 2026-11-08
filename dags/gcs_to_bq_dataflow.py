"""Per-file ingestion: register job -> Dataflow flex template -> finalize (audit, move file, error event).

Triggered by pubsub_file_listener (run_id file__<generation>__<name>) with conf:
  {bucket, name, generation, size, time_created, message_id, subscription, delivery_attempt}
A manual trigger needs only {bucket, name, generation}; it may add template_path to test a rejected launch.
See project.md section 3.
"""

import logging
import uuid
from datetime import datetime, timedelta, timezone

from airflow.configuration import conf as airflow_conf
from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from airflow.providers.google.cloud.operators.dataflow import DataflowStartFlexTemplateOperator
from airflow.utils.trigger_rule import TriggerRule

from dfa_common import config
from dfa_common.bq import run_query
from dfa_common.dataflow import error_messages, find_job
from dfa_common.events import airflow_run_url, build_error_event, publish_error_event, task_log_tail
from dfa_common.logic import archive_destination, decide_outcome, failure_message, first_failed_task

log = logging.getLogger(__name__)


def _job(field: str) -> str:
    """Jinja reference to a register_job output field (rendered when run_dataflow starts)."""
    return "{{ ti.xcom_pull(task_ids='register_job')['" + field + "'] }}"


def _tasks(dag_run) -> list:
    """Every task instance of the run, for the error event and the agent."""
    tasks = []
    for ti in sorted(dag_run.get_task_instances(), key=lambda t: t.start_date or datetime.max.replace(tzinfo=timezone.utc)):
        tasks.append({"task_id": ti.task_id, "state": str(ti.state) if ti.state else None, "try_number": ti.try_number,
                      "map_index": ti.map_index, "start_date": ti.start_date, "end_date": ti.end_date,
                      "duration": ti.duration})  # fmt: skip
    return tasks


def _orchestration(dag_run, failed_task: str, task_error: str) -> dict:
    return {
        "environment": config.COMPOSER_ENV,
        "dag_id": dag_run.dag_id,
        "dag_run_id": dag_run.run_id,
        "tasks": _tasks(dag_run),
        "failed_task": failed_task,
        "task_error": task_error,
        "console_url": airflow_run_url(airflow_conf.get("webserver", "base_url", fallback=""), dag_run.dag_id, dag_run.run_id),
    }


def _task_error(dag_run, task_id: str) -> str:
    if not task_id:
        return ""
    ti = dag_run.get_task_instance(task_id)
    return task_log_tail(dag_run.dag_id, dag_run.run_id, task_id, ti.try_number) if ti else ""


def _file(conf: dict, final_path: str = "") -> dict:
    return {"bucket": conf.get("bucket"), "name": conf.get("name"), "generation": str(conf.get("generation")),
            "original_path": f"gs://{conf.get('bucket')}/{conf.get('name')}", "final_path": final_path}  # fmt: skip


def _job_row(job_id: str) -> dict:
    rows = run_query(f"""
        SELECT job_id, status, status_reason, error_code, error_message, job_name, contract_name, target_table,
               rows_in_file, rows_loaded, validation_status, recon_status, start_time, end_time, attempt_number
        FROM {config.JOB_METADATA_TABLE} WHERE job_id = @job_id""", job_id=("STRING", job_id))  # fmt: skip
    return {k: (v.isoformat() if hasattr(v, "isoformat") else v) for k, v in dict(rows[0].items()).items()} if rows else {}


def _failed_checks(job_id: str) -> list:
    rows = run_query(f"""
        SELECT check_name, check_type, summary, ARRAY_TO_STRING(failed_checks, ',') AS failed_rules,
               SUBSTR(TO_JSON_STRING(details), 1, 2000) AS details_json
        FROM {config.VALIDATION_METADATA_TABLE}
        WHERE job_id = @job_id AND validation_result = 'FAIL' ORDER BY check_name""", job_id=("STRING", job_id))  # fmt: skip
    return [dict(r.items()) for r in rows]


def _mark_failed(job_id: str, error_code: str, message: str):
    run_query(f"""
        UPDATE {config.JOB_METADATA_TABLE}
        SET status = 'FAILED', status_reason = @reason, error_code = @error_code, error_message = @reason,
            end_time = CURRENT_TIMESTAMP(),
            duration_seconds = TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), start_time, MILLISECOND) / 1000.0,
            updated_at = CURRENT_TIMESTAMP()
        WHERE job_id = @job_id""", job_id=("STRING", job_id), reason=("STRING", message),
              error_code=("STRING", error_code))  # fmt: skip


def on_ingest_failure(context):
    """DAG-level safety net: if the run failed WITHOUT finalize publishing an error event (e.g. register_job or
    finalize itself crashed, or the run timed out), mark the job FAILED and publish a minimal event."""
    dag_run = context["dag_run"]
    try:
        finalize_ti = dag_run.get_task_instance("finalize")
        if finalize_ti and finalize_ti.xcom_pull(task_ids="finalize", key="error_event_id"):
            return  # finalize already reported this failure
        register_ti = dag_run.get_task_instance("register_job")
        job = (register_ti.xcom_pull(task_ids="register_job") if register_ti else None) or {}
        states = {ti.task_id: str(ti.state) for ti in dag_run.get_task_instances()}
        failed_task = first_failed_task(states)
        task_error = _task_error(dag_run, failed_task)
        message = f"Airflow task {failed_task or '(none)'} failed in DAG run {dag_run.run_id}. {task_error.strip()[-600:]}"
        if job.get("job_id"):
            _mark_failed(job["job_id"], "AIRFLOW_TASK_FAILED", message[:900])
        job_row = _job_row(job["job_id"]) if job.get("job_id") else {}
        event = build_error_event(
            job={**job_row, "job_id": job.get("job_id", ""), "error_code": "AIRFLOW_TASK_FAILED",
                 "error_message": message[:900], "job_name": job_row.get("job_name")},
            status="FAILED", file=_file(dag_run.conf or {}), failed_checks=[],
            dataflow={"job_id": "", "state": "NOT_STARTED", "launch_error": "", "error_messages": []},
            orchestration=_orchestration(dag_run, failed_task, task_error),
        )  # fmt: skip
        log.info("Published fallback error event %s", publish_error_event(event))
    except Exception:
        log.exception("Could not report the failure of DAG run %s", dag_run.run_id)


@dag(
    dag_id=config.INGEST_DAG_ID,
    description="Validate and load one landed GCS file into BigQuery via Dataflow.",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=20,  # runs queue on run_dataflow's slot limit, not here
    dagrun_timeout=timedelta(hours=3),
    default_args={"retries": 0},
    on_failure_callback=on_ingest_failure,
    tags=["ingestion", "dataflow"],
)
def gcs_to_bq_dataflow():
    @task
    def register_job(**context) -> dict:
        dag_run = context["dag_run"]
        conf = dag_run.conf
        bucket, name, generation = conf["bucket"], conf["name"], str(conf["generation"])
        file_path = f"gs://{bucket}/{name}"
        file_name = name.rsplit("/", 1)[-1]
        job_id = str(uuid.uuid4())

        prior = run_query(
            f"""
            SELECT COUNT(*) AS attempts, COUNTIF(status = 'SUCCESS') AS successes
            FROM {config.JOB_METADATA_TABLE}
            WHERE file_path = @file_path AND file_generation = @generation
              AND status != 'DUPLICATE_SKIPPED'
            """,
            file_path=("STRING", file_path),
            generation=("STRING", generation),
        )[0]
        duplicate = prior.successes > 0
        status = "DUPLICATE_SKIPPED" if duplicate else "IN_PROGRESS"

        run_query(
            f"""
            INSERT INTO {config.JOB_METADATA_TABLE} (
              job_id, job_name, trigger_type, pubsub_message_id, pubsub_subscription, delivery_attempt,
              attempt_number, is_retried, status, status_reason, start_time, source_bucket, file_name,
              file_path, file_generation, file_size_bytes, file_format, file_created_at,
              gcp_project, gcp_region, service_name, service_revision, created_at, updated_at
            ) VALUES (
              @job_id, @job_name, 'PUBSUB', @message_id, @subscription, @delivery_attempt,
              @attempt, @attempt > 1, @status, @reason, CURRENT_TIMESTAMP(), @bucket, @file_name,
              @file_path, @generation, @size, @file_format, @time_created,
              @project, @region, 'composer', @run_id, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
            )
            """,
            job_id=("STRING", job_id),
            job_name=("STRING", f"{file_name.split('_', 1)[0]}_load"),
            message_id=("STRING", conf.get("message_id")),
            subscription=("STRING", conf.get("subscription")),
            delivery_attempt=("INT64", conf.get("delivery_attempt")),
            attempt=("INT64", prior.attempts + 1),
            status=("STRING", status),
            reason=("STRING", "Generation already loaded successfully" if duplicate else "Registered by Airflow"),
            bucket=("STRING", bucket),
            file_name=("STRING", file_name),
            file_path=("STRING", file_path),
            generation=("STRING", generation),
            size=("INT64", conf.get("size")),
            file_format=("STRING", file_name.rsplit(".", 1)[-1].lower() if "." in file_name else None),
            time_created=("TIMESTAMP", conf.get("time_created")),
            project=("STRING", config.PROJECT_ID),
            region=("STRING", config.REGION),
            run_id=("STRING", dag_run.run_id),
        )
        return {
            "job_id": job_id,
            "duplicate": duplicate,
            "bucket": bucket,
            "name": name,
            "file_path": file_path,
            "generation": generation,
            "message_id": conf.get("message_id") or "",
        }

    @task.short_circuit(ignore_downstream_trigger_rules=True)
    def is_new_file(job: dict) -> bool:
        return not job["duplicate"]

    job = register_job()

    # max_active_tis_per_dag: at most N Dataflow jobs across all runs (external-IP quota); the rest wait here.
    run_dataflow = DataflowStartFlexTemplateOperator(
        task_id=config.DATAFLOW_TASK_ID,
        project_id=config.PROJECT_ID,
        location=config.REGION,
        wait_until_finished=True,
        append_job_name=False,  # keep the deterministic name ingest-<job_id>
        max_active_tis_per_dag=config.MAX_CONCURRENT_JOBS,
        execution_timeout=timedelta(minutes=60),
        body={
            "launchParameter": {
                "jobName": "ingest-" + _job("job_id"),
                # A manual run may override the spec (make airflow-test-launch-failure); Pub/Sub runs never do.
                "containerSpecGcsPath": "{{ dag_run.conf.get('template_path') or '" + config.TEMPLATE_PATH + "' }}",
                "parameters": {
                    "input_file": _job("file_path"),
                    "audit_job_id": _job("job_id"),
                    "file_generation": _job("generation"),
                    "pubsub_message_id": _job("message_id"),
                    "trigger_type": "PUBSUB",
                    "audit_dataset": config.AUDIT_DATASET,
                    "target_dataset": config.TARGET_DATASET,
                },
                "environment": {
                    "serviceAccountEmail": config.DATAFLOW_SA,
                    "tempLocation": f"gs://{config.DATAFLOW_BUCKET}/temp",
                    "stagingLocation": f"gs://{config.DATAFLOW_BUCKET}/staging",
                    "maxWorkers": 1,
                    "additionalUserLabels": {**config.DATAFLOW_LABEL, "audit_job_id": _job("job_id")},
                },
            }
        },
    )

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def finalize(job: dict, **context):
        """Reconciles Airflow and Dataflow outcomes, moves the file, publishes the error event on failure."""
        dag_run, ti = context["dag_run"], context["ti"]
        job_id = job["job_id"]
        dataflow_ti = dag_run.get_task_instance(config.DATAFLOW_TASK_ID)
        dataflow_task_state = str(dataflow_ti.state) if dataflow_ti and dataflow_ti.state else None

        try:
            dataflow_job = find_job(config.dataflow_job_name(job_id))
        except Exception:
            log.exception("Could not look up the Dataflow job for %s", job_id)
            dataflow_job = {}

        current = run_query(f"SELECT status FROM {config.JOB_METADATA_TABLE} WHERE job_id = @job_id",
                            job_id=("STRING", job_id))  # fmt: skip
        status, error_code = decide_outcome(dataflow_job, current[0].status if current else None)

        states = {t.task_id: str(t.state) for t in dag_run.get_task_instances()}
        failed_task = first_failed_task(states, exclude=("finalize",))
        task_error = _task_error(dag_run, failed_task)
        if error_code:
            _mark_failed(job_id, error_code, failure_message(dataflow_job, dataflow_task_state, task_error))

        destination = archive_destination(job["name"], status, datetime.now(timezone.utc))
        final_path = f"gs://{job['bucket']}/{destination}"
        _move_file(job["bucket"], job["name"], int(job["generation"]), destination)
        run_query(f"""
            UPDATE {config.JOB_METADATA_TABLE} SET final_file_path = @final_path, updated_at = CURRENT_TIMESTAMP()
            WHERE job_id = @job_id""", job_id=("STRING", job_id), final_path=("STRING", final_path))  # fmt: skip

        if status in ("FAILED", "REJECTED"):
            try:
                dataflow_errors = error_messages(dataflow_job["id"]) if dataflow_job.get("id") else []
                event = build_error_event(
                    job=_job_row(job_id), status=status, file=_file(dag_run.conf, final_path),
                    failed_checks=_failed_checks(job_id),
                    dataflow={"job_id": dataflow_job.get("id", ""), "state": dataflow_job.get("state", "NOT_STARTED"),
                              "launch_error": task_error if not dataflow_job else "", "error_messages": dataflow_errors},
                    orchestration=_orchestration(dag_run, failed_task, task_error),
                )  # fmt: skip
                ti.xcom_push(key="error_event_id", value=publish_error_event(event))
            except Exception:
                # Publishing problems never change the job outcome.
                log.exception("Could not publish the error event for job %s", job_id)

        if status == "FAILED":
            raise AirflowFailException(f"Ingestion failed for {job['file_path']} (job_id={job_id})")
        return status

    is_new_file(job) >> run_dataflow >> finalize(job)


def _move_file(bucket_name: str, name: str, generation: int, destination: str):
    """copy + delete (only the generation we processed). A missing object was already moved: nothing to do."""
    from airflow.providers.google.cloud.hooks.gcs import GCSHook
    from google.api_core.exceptions import NotFound, PreconditionFailed

    bucket = GCSHook().get_conn().bucket(bucket_name)
    blob = bucket.blob(name)
    try:
        bucket.copy_blob(blob, bucket, destination, source_generation=generation)
        blob.delete(if_generation_match=generation)
    except (NotFound, PreconditionFailed):
        log.warning("gs://%s/%s generation %s is gone or replaced; not moved", bucket_name, name, generation)


gcs_to_bq_dataflow()
