# System: GCS → BigQuery ingestion pipeline (for incident agents)

## What the pipeline does
A delimited CSV file lands in `gs://<project>-dfa-landing/landing/<file>`. A GCS notification goes to the
Pub/Sub topic `dfa-file-landed`. An Eventarc trigger then starts one **Cloud Workflows** execution
(`dfa-ingest-file`) per file. The workflow:
1. Claims the delivery (`gs://<project>-dfa-dataflow/claims/...`) so duplicate Pub/Sub deliveries are skipped.
2. Inserts a `job_metadata` row with status `IN_PROGRESS` (`job_id` = UUID).
3. Waits for a Dataflow slot. Only `max_concurrent_jobs` (default 1) jobs run at once, using lock objects in
   `gs://<project>-dfa-dataflow/locks/`. This keeps the project inside its external-IP quota (4 in europe-west2).
4. Launches the **Dataflow Flex Template** `gcs-to-bq` (Apache Beam, Python) with `input_file` and `audit_job_id`.
5. Polls the Dataflow job until it reaches a terminal state (max 60 min), then releases the slot.
6. Finalizes: reads the status that Dataflow wrote. If the Dataflow job did not end `JOB_STATE_DONE`, or the status is
   still `IN_PROGRESS`, it sets `FAILED` (`error_code = DATAFLOW_JOB_FAILED`). It moves the file to
   `archive/YYYY/MM/DD/` (SUCCESS) or `invalid/YYYY/MM/DD/` (anything else).
7. On FAILED or REJECTED, publishes an error event to `dfa-ingest-errors`. That event is your incident.

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
| Orchestration | Workflow execution of `dfa-ingest-file` (its state and error payload) |
| The file | `invalid/YYYY/MM/DD/<file>` after a failure |
| Previous agent actions | BigQuery `file_audit.agent_incidents` |

## How a retry works
Retrying means **copying** the file from `invalid/…/<file>` back to `landing/<file>`. That creates a new
generation, so the normal flow processes it again as a new job_id. Never move or delete the original.
