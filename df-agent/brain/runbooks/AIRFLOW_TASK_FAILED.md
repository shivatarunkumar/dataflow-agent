# Runbook: AIRFLOW_TASK_FAILED (failure in the Airflow layer)
**Category:** depends on the error · **Status:** FAILED · **error_code:** `AIRFLOW_TASK_FAILED` or `DATAFLOW_JOB_FAILED`

## How to confirm (top-down)
1. `get_airflow_dag_run`: the run of `gcs_to_bq_dataflow` (`orchestration.dag_run_id`). State, start and end times.
2. `get_airflow_task_instances`: which task failed first? Task order: `register_job` → `is_new_file` → `run_dataflow` → `finalize`.
   A long gap between `queued_when` and `start_date` on `run_dataflow` means the file waited for a Dataflow slot.
3. `get_airflow_task_log` on that task: the exception is at the end of the log.
4. `find_dataflow_job`: did a Dataflow job `ingest-<job_id>` get created at all?

## Classify
| Evidence | Category |
|---|---|
| `register_job` or `finalize` failed with a frame in `dags/…py` (our DAG code) | CODE_BUG |
| `register_job`/`finalize` failed with a BigQuery `403` / `Access Denied` | CONFIG (see PERMISSION_DENIED) |
| `Detected zombie job`, `was killed externally`, `received SIGTERM` | TRANSIENT_INFRA (Composer worker restarted) |
| `AirflowTaskTimeout` on `run_dataflow` (job ran > 60 min and was cancelled) | TRANSIENT_INFRA, low confidence: a human decides |
| `run_dataflow` failed and **no** Dataflow job exists | launch failure: see DATAFLOW_LAUNCH_FAILED |
| `run_dataflow` failed and the Dataflow job is FAILED | not an Airflow problem: continue with DATAFLOW_JOB_FAILED |

## Action
- TRANSIENT_INFRA (zombie) → `RETRY_FILE`.
- CODE_BUG → `PROPOSE_CODE_CHANGE` in `dags/` (name the task and function), then `make dags-deploy` and retry.
- Otherwise → `ESCALATE` with the task log tail.
