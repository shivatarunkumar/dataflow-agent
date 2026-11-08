# Glossary

| Term | Meaning |
|---|---|
| job_id | UUID for one processing attempt of one file generation (= incident_id) |
| generation | GCS object version. A re-upload or retry copy creates a new generation and a new job |
| contract | YAML describing an entity's file (name pattern, delimiter, columns, target table) |
| REJECTED | Data-contract failure, the file's fault. Never auto-retried |
| FAILED | Platform or code failure. Auto-retried only if transient |
| DATAFLOW_JOB_FAILED | error_code set by the Airflow `finalize` task when the Dataflow job did not finish DONE |
| AIRFLOW_TASK_FAILED | error_code when the failure happened in Airflow before/without a Dataflow job (launch rejected, register/finalize crash, run timeout) |
| dag_run_id | Airflow run of `gcs_to_bq_dataflow` for one file generation: `file__<generation>__<name>` (= job_metadata.service_revision) |
| failing layer | Where the failure happened, top-down: AIRFLOW, DATAFLOW, PIPELINE (recon after a DONE job), DATA (REJECTED) |
| RECON_MISMATCH | error_code when file rows, trailer count and BigQuery rows disagree after the load |
| LAUNCH_OR_POLL_FAILED / POLL_TIMEOUT | v1 (Cloud Workflows) dataflow.state values; only seen when replaying old incidents |
| NOT_STARTED | dataflow.state when no Dataflow job was created |
| TRANSIENT_INFRA | Category: temporary platform issue (quota, VM, internal error); a retry is expected to work |
| DATA_QUALITY | Category: the file content or name is wrong; the data owner must fix it |
| CODE_BUG | Category: pipeline code defect; needs a code change |
| CONFIG | Category: infra or config problem (permissions, missing template, quota limit too low) |
| UNKNOWN | Category: not enough evidence; escalate to a human |
