# System: GCS → BigQuery ingestion pipeline (for incident agents)

## What the pipeline does
A delimited CSV file lands in `gs://<project>-dfa-landing/landing/<file>`. A GCS notification goes to the Pub/Sub
topic `dfa-file-landed` and waits on the pull subscription `dfa-file-landed-airflow`. **Apache Airflow (Cloud
Composer 3, environment `dfa-composer`)** orchestrates:
1. DAG `pubsub_file_listener` (every 2 min) pulls the notifications and triggers one run of DAG `gcs_to_bq_dataflow`
   per file generation, with `dag_run_id = file__<generation>__<file name>`. A duplicate delivery maps to the same
   run id and is skipped.
2. Task `register_job` inserts a `job_metadata` row with status `IN_PROGRESS` (`job_id` = UUID,
   `service_name = 'composer'`, `service_revision = <dag_run_id>`).
3. Task `run_dataflow` launches the **Dataflow Flex Template** `gcs-to-bq` (Apache Beam, Python) as job
   `ingest-<job_id>` and waits for it (max 60 min, then Airflow cancels it). Only `max_concurrent_jobs` (default 1) of
   these tasks run at once; other files wait as *scheduled* task instances. This keeps the project inside its
   external-IP quota (4 in europe-west2; each Dataflow job uses 2).
4. Task `finalize` (always runs) reads the status that Dataflow wrote. If the Dataflow job did not end
   `JOB_STATE_DONE`, or the status is still `IN_PROGRESS`, it sets `FAILED` with `error_code = DATAFLOW_JOB_FAILED`,
   or `AIRFLOW_TASK_FAILED` when no Dataflow job was created (e.g. the launch was rejected). It moves the file to
   `archive/YYYY/MM/DD/` (SUCCESS) or `invalid/YYYY/MM/DD/` (anything else).
5. On FAILED or REJECTED, `finalize` publishes an error event (schema v2) to `dfa-ingest-errors`. If the run fails
   without `finalize` doing that (e.g. `register_job` crashed), a DAG failure callback publishes a minimal event.
   That event is your incident.

## Investigate top-down: Airflow → Dataflow → BigQuery
1. **Airflow:** `get_airflow_dag_run`, `get_airflow_task_instances`, then `get_airflow_task_log` for the first failed
   task. If a task other than `run_dataflow` failed, or `run_dataflow` failed and `find_dataflow_job` finds no job, the
   failure is in the **AIRFLOW** layer.
2. **Dataflow:** `get_dataflow_job`, `get_dataflow_errors`, `get_logs`. A FAILED/CANCELLED job is the **DATAFLOW**
   layer; the failing step and the exception say why.
3. **BigQuery audit:** a DONE Dataflow job with status FAILED is the **PIPELINE** layer (reconciliation); REJECTED is
   the **DATA** layer (see failed checks).

## Dataflow job checks (all ERROR severity: any failure rejects the whole file)
| check_name | What it verifies |
|---|---|
| contract_match | File-name prefix maps to a contract (customer, user, orders) |
| file_name | `<entity>_<YYYYMMDD>.csv` with a valid date |
| header | Header columns equal the contract columns, in order |
| delimiter | Header and every row split into the expected field count with the contract delimiter |
| trailer | Last line `T|<count>|<YYYYMMDD>`; count = data rows; date = file date |
| data_type | NOT_NULL and type parsing per column |
| recon_row_count | After load: file rows = trailer count = rows in BigQuery for the job_id |

A failed pre-load check means the job ends **REJECTED**: nothing is loaded and the file goes to `invalid/`. A file
that passes the checks but fails reconciliation, or a crashed Dataflow job, ends **FAILED**.

## Status meanings
- `SUCCESS`: loaded and reconciled.
- `REJECTED`: the file broke the data contract. This is a **data problem**. Retrying the same file will fail again;
  the data owner must fix the file.
- `FAILED`: the platform or code failed (Dataflow crash, launch/quota error, recon mismatch). It may be
  **transient** (quota, VM start-up, internal errors), in which case a retry can work, or a **bug/config** issue.
- `DUPLICATE_SKIPPED`: the same generation was already loaded.

## Where evidence lives
| Evidence | Location |
|---|---|
| Job record | BigQuery `file_audit.job_metadata` (one row per job_id) |
| Check results | BigQuery `file_audit.validation_metadata` (one row per check per job_id) |
| Loaded data | BigQuery `raw.<contract>` (lineage columns `_job_id`, `_source_file`, `_loaded_at`) |
| Dataflow errors | Dataflow job messages (JOB_MESSAGE_ERROR) and Cloud Logging `resource.type="dataflow_step"` |
| Orchestration | Airflow DAG run `gcs_to_bq_dataflow` / `<dag_run_id>`: task states and task logs (Airflow REST API) |
| Launched job | Dataflow job named `ingest-<job_id>` (`find_dataflow_job`) |
| The file | `invalid/YYYY/MM/DD/<file>` after a failure |
| Previous agent actions | BigQuery `file_audit.agent_incidents` |

## How a retry works
Retrying means **copying** the file from `invalid/…/<file>` back to `landing/<file>`. That creates a new
generation, so the normal flow processes it again as a new job_id. Never move or delete the original.
