"""Per-file ingestion: register job -> Dataflow flex template -> finalize (audit + move file).

Triggered by pubsub_file_listener with conf:
  {bucket, name, generation, size, time_created, message_id, subscription, delivery_attempt}
"""

import uuid
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.operators.dataflow import DataflowStartFlexTemplateOperator
from airflow.utils.state import TaskInstanceState
from airflow.utils.trigger_rule import TriggerRule
from google.cloud import bigquery

from dfa_common import config


def bq_client() -> bigquery.Client:
    return BigQueryHook(use_legacy_sql=False).get_client(project_id=config.PROJECT_ID, location=config.REGION)


def run_dml(sql: str, **params) -> list:
    """Runs a parameterised query. params: name -> (bq_type, value)."""
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter(k, t, v) for k, (t, v) in params.items()]
    )
    return list(bq_client().query(sql, job_config=job_config).result())


@dag(
    dag_id=config.INGEST_DAG_ID,
    description="Validate and load one landed GCS file into BigQuery via Dataflow.",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=10,
    default_args={"retries": 0},
    tags=["ingestion", "dataflow"],
)
def gcs_to_bq_dataflow():
    @task
    def register_job(**context) -> dict:
        conf = context["dag_run"].conf
        bucket, name, generation = conf["bucket"], conf["name"], str(conf["generation"])
        file_path = f"gs://{bucket}/{name}"
        file_name = name.rsplit("/", 1)[-1]
        job_id = str(uuid.uuid4())

        prior = run_dml(
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

        run_dml(
            f"""
            INSERT INTO {config.JOB_METADATA_TABLE} (
              job_id, job_name, trigger_type, pubsub_message_id, pubsub_subscription, delivery_attempt,
              attempt_number, is_retried, status, status_reason, start_time, source_bucket, file_name,
              file_path, file_generation, file_size_bytes, file_format, file_created_at,
              gcp_project, gcp_region, service_name, created_at, updated_at
            ) VALUES (
              @job_id, @job_name, 'PUBSUB', @message_id, @subscription, @delivery_attempt,
              @attempt, @attempt > 1, @status, @reason, CURRENT_TIMESTAMP(), @bucket, @file_name,
              @file_path, @generation, @size, @file_format, @time_created,
              @project, @region, 'composer', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
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
        )
        return {
            "job_id": job_id,
            "duplicate": duplicate,
            "bucket": bucket,
            "name": name,
            "file_path": file_path,
            "generation": generation,
            "message_id": conf.get("message_id"),
        }

    @task.short_circuit
    def is_new_file(job: dict) -> bool:
        return not job["duplicate"]

    job = register_job()

    run_dataflow = DataflowStartFlexTemplateOperator(
        task_id="run_dataflow",
        project_id=config.PROJECT_ID,
        location=config.REGION,
        wait_until_finished=True,
        body={
            "launchParameter": {
                "jobName": "ingest-{{ ti.xcom_pull(task_ids='register_job')['job_id'][:8] }}-{{ ts_nodash | lower }}",
                "containerSpecGcsPath": config.TEMPLATE_PATH,
                "parameters": {
                    "input_file": "{{ ti.xcom_pull(task_ids='register_job')['file_path'] }}",
                    "job_id": "{{ ti.xcom_pull(task_ids='register_job')['job_id'] }}",
                    "file_generation": "{{ ti.xcom_pull(task_ids='register_job')['generation'] }}",
                    "pubsub_message_id": "{{ ti.xcom_pull(task_ids='register_job')['message_id'] }}",
                    "trigger_type": "PUBSUB",
                    "audit_dataset": config.AUDIT_DATASET,
                    "target_dataset": config.TARGET_DATASET,
                },
                "environment": {
                    "serviceAccountEmail": config.DATAFLOW_SA,
                    "tempLocation": f"gs://{config.DATAFLOW_BUCKET}/temp",
                    "stagingLocation": f"gs://{config.DATAFLOW_BUCKET}/staging",
                    "maxWorkers": 2,
                    "additionalUserLabels": {"pipeline": "gcs-to-bq"},
                },
            }
        },
    )

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def finalize(job: dict, **context):
        """Reconciles Airflow and Dataflow outcomes, then moves the file to archive/ or invalid/."""
        dataflow_state = context["dag_run"].get_task_instance("run_dataflow").state
        rows = run_dml(
            f"SELECT status FROM {config.JOB_METADATA_TABLE} WHERE job_id = @job_id",
            job_id=("STRING", job["job_id"]),
        )
        status = rows[0].status if rows else None

        if dataflow_state != TaskInstanceState.SUCCESS or status in (None, "IN_PROGRESS"):
            status = "FAILED"
            run_dml(
                f"""
                UPDATE {config.JOB_METADATA_TABLE}
                SET status = 'FAILED', status_reason = @reason, error_code = 'DATAFLOW_JOB_FAILED',
                    error_message = @reason, end_time = CURRENT_TIMESTAMP(),
                    duration_seconds = TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), start_time, MILLISECOND) / 1000.0,
                    updated_at = CURRENT_TIMESTAMP()
                WHERE job_id = @job_id
                """,
                job_id=("STRING", job["job_id"]),
                reason=("STRING", f"Dataflow task ended in state {dataflow_state}"),
            )

        prefix = config.ARCHIVE_PREFIX if status == "SUCCESS" else config.INVALID_PREFIX
        relative = job["name"][len(config.LANDING_PREFIX):]
        destination = f"{prefix}{datetime.utcnow():%Y/%m/%d}/{relative}"

        gcs = GCSHook()
        if gcs.exists(job["bucket"], job["name"]):
            # move = copy + delete; the copy keeps the original object metadata.
            gcs.copy(job["bucket"], job["name"], job["bucket"], destination)
            gcs.delete(job["bucket"], job["name"])
            run_dml(
                f"""
                UPDATE {config.JOB_METADATA_TABLE}
                SET final_file_path = @final_path, updated_at = CURRENT_TIMESTAMP()
                WHERE job_id = @job_id
                """,
                job_id=("STRING", job["job_id"]),
                final_path=("STRING", f"gs://{job['bucket']}/{destination}"),
            )

        if status == "FAILED":
            raise AirflowFailException(f"Ingestion failed for {job['file_path']} (job_id={job['job_id']})")
        return status

    is_new_file(job) >> run_dataflow >> finalize(job)


gcs_to_bq_dataflow()
