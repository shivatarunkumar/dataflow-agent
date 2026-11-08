"""Settings injected into Composer as environment variables by infra/composer.tf."""

import os

PROJECT_ID = os.environ.get("DFA_PROJECT_ID", "")
REGION = os.environ.get("DFA_REGION", "europe-west2")
LANDING_BUCKET = os.environ.get("DFA_LANDING_BUCKET", "")
DATAFLOW_BUCKET = os.environ.get("DFA_DATAFLOW_BUCKET", "")
SUBSCRIPTION = os.environ.get("DFA_SUBSCRIPTION", "")
ERROR_TOPIC = os.environ.get("DFA_ERROR_TOPIC", "")
TEMPLATE_PATH = os.environ.get("DFA_TEMPLATE_PATH", "")
AUDIT_DATASET = os.environ.get("DFA_AUDIT_DATASET", "file_audit")
TARGET_DATASET = os.environ.get("DFA_TARGET_DATASET", "raw")
DATAFLOW_SA = os.environ.get("DFA_DATAFLOW_SA", "")
MAX_CONCURRENT_JOBS = int(os.environ.get("DFA_MAX_CONCURRENT_JOBS", "1"))
COMPOSER_ENV = os.environ.get("DFA_COMPOSER_ENV", "")
# Set by Composer: the environment bucket (holds dags/ and logs/).
COMPOSER_BUCKET = os.environ.get("GCS_BUCKET", "")

LANDING_PREFIX = "landing/"
ARCHIVE_PREFIX = "archive/"
INVALID_PREFIX = "invalid/"

JOB_METADATA_TABLE = f"`{PROJECT_ID}.{AUDIT_DATASET}.job_metadata`"
VALIDATION_METADATA_TABLE = f"`{PROJECT_ID}.{AUDIT_DATASET}.validation_metadata`"
LISTENER_DAG_ID = "pubsub_file_listener"
INGEST_DAG_ID = "gcs_to_bq_dataflow"
DATAFLOW_TASK_ID = "run_dataflow"
DATAFLOW_LABEL = {"pipeline": "gcs-to-bq"}


def dataflow_job_name(job_id: str) -> str:
    """Deterministic Dataflow job name, so finalize and the agent can find the job even if launching failed."""
    return f"ingest-{job_id}"
