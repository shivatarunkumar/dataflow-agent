"""Settings injected into Composer as environment variables by infra/composer.tf."""

import os

PROJECT_ID = os.environ.get("DFA_PROJECT_ID", "")
REGION = os.environ.get("DFA_REGION", "europe-west2")
LANDING_BUCKET = os.environ.get("DFA_LANDING_BUCKET", "")
DATAFLOW_BUCKET = os.environ.get("DFA_DATAFLOW_BUCKET", "")
SUBSCRIPTION = os.environ.get("DFA_SUBSCRIPTION", "")
TEMPLATE_PATH = os.environ.get("DFA_TEMPLATE_PATH", "")
AUDIT_DATASET = os.environ.get("DFA_AUDIT_DATASET", "file_audit")
TARGET_DATASET = os.environ.get("DFA_TARGET_DATASET", "raw")
DATAFLOW_SA = os.environ.get("DFA_DATAFLOW_SA", "")

LANDING_PREFIX = "landing/"
ARCHIVE_PREFIX = "archive/"
INVALID_PREFIX = "invalid/"

JOB_METADATA_TABLE = f"`{PROJECT_ID}.{AUDIT_DATASET}.job_metadata`"
INGEST_DAG_ID = "gcs_to_bq_dataflow"
