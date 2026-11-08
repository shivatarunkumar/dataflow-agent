# Runbook: DATAFLOW_LAUNCH_FAILED (Flex Template launch rejected)
**Category:** CONFIG · **Status:** FAILED · **error_code:** `AIRFLOW_TASK_FAILED`

## How to confirm
1. `get_airflow_task_log` on `run_dataflow`: an `HttpError 400` (or `INVALID_ARGUMENT`) from
   `flexTemplates:launch`, before any job id is printed.
2. `find_dataflow_job`: confirms that no Dataflow job `ingest-<job_id>` was created.
3. `get_gcs_object` on `templates/gcs_to_bq.json`: if the spec is missing, use TEMPLATE_MISSING instead.

## Likely causes
- A launch parameter the template doesn't declare, or a missing required one (`dataflow-job/metadata.json` vs the
  `run_dataflow` body in `dags/gcs_to_bq_dataflow.py`).
- An invalid environment value (service account, temp location, label value).
- 403 → PERMISSION_DENIED (e.g. `dfa-composer` lost `iam.serviceAccountUser` on `dfa-dataflow`).

## Action
`PROPOSE_CONFIG_CHANGE`: fix the parameters, run `make template` (spec) or `make dags-deploy` (DAG), then retry the file.
