"""Audit trail: validation_metadata rows (one per check) and the final job_metadata MERGE."""

import json
import uuid
from datetime import date

import apache_beam as beam
from apache_beam.io.gcp.bigquery import BigQueryDisposition, WriteToBigQuery

from pipeline import PIPELINE_VERSION, checks


class ToValidationRow(beam.DoFn):
    def __init__(self, project: str, target_dataset: str, job_id: str):
        self._project = project
        self._target_dataset = target_dataset
        self._job_id = job_id

    def process(self, result, meta):
        contract = meta["contract"]
        yield {
            "validation_id": str(uuid.uuid4()),
            "job_id": self._job_id,
            "file_name": meta["file_name"],
            "target_table": (
                f"{self._project}.{self._target_dataset}.{contract.target_table}" if contract else None
            ),
            "contract_name": contract.name if contract else None,
            "contract_version": contract.version if contract else None,
            "stage": result.stage,
            "check_name": result.check_name,
            "check_type": result.check_type,
            "validation_result": result.result,
            "severity": result.severity,
            "summary": result.summary,
            "records_evaluated": result.records_evaluated,
            "records_failed": result.records_failed,
            "records_warned": 0,
            "checks_evaluated": result.checks_evaluated,
            "checks_failed": len(result.failed_checks),
            "failed_checks": result.failed_checks,
            # JSON columns are sent as JSON strings with streaming inserts.
            "details": json.dumps(result.details, default=str),
            "check_config": json.dumps(result.check_config, default=str),
            "validated_at": result.validated_at.isoformat(),
            "created_at": checks.utc_now().isoformat(),
        }


class WriteValidationMetadata(beam.PTransform):
    def __init__(self, project: str, audit_dataset: str, target_dataset: str, job_id: str, meta):
        super().__init__()
        self._project = project
        self._audit_dataset = audit_dataset
        self._target_dataset = target_dataset
        self._job_id = job_id
        self._meta = meta

    def expand(self, results):
        return (
            results
            | "ToValidationRow" >> beam.ParDo(
                ToValidationRow(self._project, self._target_dataset, self._job_id), meta=self._meta
            )
            | "WriteValidationMetadata" >> WriteToBigQuery(
                table=f"{self._project}:{self._audit_dataset}.validation_metadata",
                method=WriteToBigQuery.Method.STREAMING_INSERTS,
                create_disposition=BigQueryDisposition.CREATE_NEVER,
                write_disposition=BigQueryDisposition.WRITE_APPEND,
            )
        )


def summarise_job(meta, results, rows_in_file, rows_failed_validation):
    """Derives job_metadata status fields from all ValidationResults."""
    pre_load = [r for r in results if r.stage == checks.PRE_LOAD]
    recon = next((r for r in results if r.check_name == "recon_row_count"), None)
    blocking = [r for r in pre_load if r.is_blocking]
    rows_loaded = recon.details.get("rows_in_bq", 0) if recon else 0

    if blocking:
        status = "REJECTED"
        error_code = blocking[0].check_name.upper()
        error_message = "; ".join(r.summary for r in blocking)[:1000]
        reason = f"{len(blocking)} validation check(s) failed: " + ", ".join(r.check_name for r in blocking)
    elif recon is None or recon.result == checks.FAIL:
        status = "FAILED"
        error_code = "RECON_MISMATCH"
        error_message = recon.summary if recon else "Reconciliation did not run"
        reason = error_message
    else:
        status, error_code, error_message = "SUCCESS", None, None
        reason = f"Loaded {rows_loaded} rows"

    contract = meta["contract"]
    return {
        "status": status,
        "status_reason": reason,
        "error_code": error_code,
        "error_message": error_message,
        "job_name": f"{contract.name}_load" if contract else "unmapped",
        "validation_status": "FAILED" if blocking else "PASSED",
        "validation_rules_executed": len(pre_load),
        "validation_rules_failed": len([r for r in pre_load if r.result == checks.FAIL]),
        "recon_status": {checks.PASS: "PASSED", checks.FAIL: "FAILED"}.get(recon.result, "SKIPPED") if recon else "SKIPPED",
        "rows_in_file": rows_in_file,
        "rows_loaded": rows_loaded,
        "rows_failed_validation": rows_failed_validation,
        "bq_load_job_id": (",".join(recon.details.get("bq_job_ids", [])) or None) if recon else None,
    }


class FinalizeJobMetadata(beam.DoFn):
    """MERGEs the final job_metadata row (updates the workflow's IN_PROGRESS row or inserts for manual runs).

    Uses DML rather than streaming inserts so the row stays updatable by the workflow afterwards.
    """

    def __init__(self, project: str, region: str, audit_dataset: str, target_dataset: str, job_id: str, run: dict):
        self._project = project
        self._region = region
        self._audit_dataset = audit_dataset
        self._target_dataset = target_dataset
        self._job_id = job_id
        self._run = run
        self._client = None

    def setup(self):
        from google.cloud import bigquery

        self._client = bigquery.Client(project=self._project)

    def process(self, _, meta, results, rows_in_file, delimiter_errors, type_errors):
        from google.cloud import bigquery

        summary = summarise_job(meta, results, rows_in_file, delimiter_errors["count"] + type_errors["count"])
        contract = meta["contract"]

        # (BigQuery type, value) per column written on both update and insert.
        values = {
            "job_name": ("STRING", summary["job_name"]),
            "status": ("STRING", summary["status"]),
            "status_reason": ("STRING", summary["status_reason"]),
            "source_bucket": ("STRING", meta["source_bucket"]),
            "file_name": ("STRING", meta["file_name"]),
            "file_path": ("STRING", meta["file_path"]),
            "file_size_bytes": ("INT64", meta["file_size_bytes"]),
            "file_format": ("STRING", meta["file_format"]),
            "file_date": ("DATE", date.fromisoformat(meta["file_date"]) if meta["file_date"] else None),
            "contract_name": ("STRING", contract.name if contract else None),
            "contract_version": ("STRING", contract.version if contract else None),
            "target_table": (
                "STRING",
                f"{self._project}.{self._target_dataset}.{contract.target_table}" if contract else None,
            ),
            "load_mode": ("STRING", contract.load_mode if contract else None),
            "bq_load_job_id": ("STRING", summary["bq_load_job_id"]),
            "rows_in_file": ("INT64", summary["rows_in_file"]),
            "rows_loaded": ("INT64", summary["rows_loaded"]),
            "rows_failed_validation": ("INT64", summary["rows_failed_validation"]),
            "validation_status": ("STRING", summary["validation_status"]),
            "validation_rules_executed": ("INT64", summary["validation_rules_executed"]),
            "validation_rules_failed": ("INT64", summary["validation_rules_failed"]),
            "recon_status": ("STRING", summary["recon_status"]),
            "error_code": ("STRING", summary["error_code"]),
            "error_message": ("STRING", summary["error_message"]),
            "gcp_project": ("STRING", self._project),
            "gcp_region": ("STRING", self._region),
            "service_version": ("STRING", PIPELINE_VERSION),
        }
        # Only set on insert: the workflow owns these when it registered the job.
        insert_only = {
            "service_name": ("STRING", "dataflow"),
            "trigger_type": ("STRING", self._run.get("trigger_type")),
            "pubsub_message_id": ("STRING", self._run.get("pubsub_message_id")),
            "file_generation": ("STRING", self._run.get("file_generation")),
            "attempt_number": ("INT64", 1),
            "is_retried": ("BOOL", False),
        }

        table = f"`{self._project}.{self._audit_dataset}.job_metadata`"
        set_clause = ",\n  ".join(f"{col} = @{col}" for col in values)
        insert_cols = list(values) + list(insert_only)
        query = f"""
MERGE {table} T
USING (SELECT @job_id AS job_id) S
ON T.job_id = S.job_id
WHEN MATCHED THEN UPDATE SET
  {set_clause},
  end_time = CURRENT_TIMESTAMP(),
  duration_seconds = TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), COALESCE(T.start_time, @start_time), MILLISECOND) / 1000.0,
  updated_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT
  (job_id, {", ".join(insert_cols)}, start_time, end_time, duration_seconds, created_at, updated_at)
VALUES
  (@job_id, {", ".join("@" + c for c in insert_cols)}, @start_time, CURRENT_TIMESTAMP(),
   TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), @start_time, MILLISECOND) / 1000.0,
   CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP())
"""
        params = [
            bigquery.ScalarQueryParameter("job_id", "STRING", self._job_id),
            bigquery.ScalarQueryParameter("start_time", "TIMESTAMP", meta["start_time"]),
        ] + [
            bigquery.ScalarQueryParameter(name, bq_type, value)
            for name, (bq_type, value) in {**values, **insert_only}.items()
        ]
        self._client.query(query, job_config=bigquery.QueryJobConfig(query_parameters=params)).result()
        yield {"job_id": self._job_id, **summary}
