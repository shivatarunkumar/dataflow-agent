# GCS → BigQuery Ingestion (Dataflow + Cloud Workflows)

## 1. Overview
Files dropped into a GCS landing bucket are validated and loaded into BigQuery by an Apache Beam (Python) Dataflow job. Orchestration is serverless. A GCS → Pub/Sub notification fires an Eventarc trigger, which starts one **Cloud Workflows** execution per file. The workflow registers the job, launches the Dataflow Flex Template, waits for it to finish, then finalizes the audit record and moves the file. Every run and every validation check is recorded in BigQuery audit tables.

```
 ┌──────────────┐ OBJECT_FINALIZE ┌──────────────┐  Eventarc  ┌────────────────────────────────┐
 │ GCS landing/ │ ──────────────▶ │ Pub/Sub topic│ ─────────▶ │ Workflow execution (per file)  │
 └──────────────┘                 └──────────────┘            │ register → launch → poll →     │
                                                              │ finalize (audit + move file)   │
                                                              └───────────────┬────────────────┘
                                                                              │ Flex Template
                                                              ┌───────────────▼────────────────┐
                                                              │ Dataflow (Beam Python)         │
                                                              │ validate → load → recon        │
                                                              └───────┬────────────────┬───────┘
                                                                      │                │
                                                        raw.<table> ◀─┘                └─▶ file_audit.job_metadata
                                                                                           file_audit.validation_metadata
```

## 2. Key decisions
| Topic | Decision |
|---|---|
| GCP project | `bigquerytarun` (`PROJECT_ID` default in the Makefile; override with `make <target> PROJECT_ID=...`) |
| Region | `europe-west2` |
| File format | Delimited CSV only. Other extensions (e.g. `.xlsx`) fail the file-name check. |
| File name | `<entity>_<YYYYMMDD>.csv`, e.g. `customer_20261015.csv` |
| Trailer | Last line `T|<record_count>|<YYYYMMDD>`. The count must equal the number of data rows, and the date must equal the file-name date. |
| Failure handling | Any ERROR-level check failure rejects the **whole file**. Nothing is loaded, the file moves to `invalid/`, and the status is `REJECTED`. |
| Load mode | Append only |
| Orchestration | **Cloud Workflows + Eventarc**, replacing Composer/Airflow. Costs about $0 at this volume, with nothing running between files. |
| Dataflow packaging | Flex Template (image in Artifact Registry, spec in GCS) |
| Concurrency | **One Dataflow job at a time** (`max_concurrent_jobs = 1`), with `maxWorkers = 1`. A job uses at most 2 external IPs (launcher VM + 1 worker), which stays within the project's `IN_USE_ADDRESSES` quota of 4 in europe-west2. Other files queue inside their workflow execution and run one by one. |
| IaC | Terraform in `infra/`, with local state. The workflow YAML is deployed by Terraform. |

**Why Workflows over Composer:** Composer 3 small costs about $20/day even when idle. Workflows is billed per step: 5,000 steps a month are free, and each file uses about 40–50 steps. It deploys in seconds, needs no code packaging, and keeps a per-file execution history (inputs, every step, errors), similar to Airflow DAG runs.

## 3. End-to-end flow
1. **File lands** at `gs://<project>-dfa-landing/landing/<file>`.
2. **GCS notification** (OBJECT_FINALIZE, prefix `landing/`) publishes to topic `dfa-file-landed`.
3. **Eventarc trigger** `dfa-file-landed` (Pub/Sub transport on that topic) starts workflow `dfa-ingest-file`, passing the CloudEvent.
4. **Workflow `dfa-ingest-file`** (`workflows/ingest_file.yaml`):
   1. `parse_event`: base64-decodes the GCS object JSON and reads bucket, name, generation, size and timeCreated, plus the Pub/Sub messageId. It ignores anything outside `landing/` and folder markers.
   2. `claim_delivery`: creates `gs://<dataflow bucket>/claims/<bucket>/<object>@<generation>` with `ifGenerationMatch=0`. Pub/Sub delivers at least once, so the same message can start two executions. Only the first claim succeeds. A duplicate delivery gets HTTP 412, stops, and writes nothing to BigQuery. This guarantees **one job_id and one job_metadata row per file generation**, with all of that job's checks in validation_metadata under the same job_id. Claims expire after 8 days, which is longer than Pub/Sub's 7-day maximum redelivery window.
   3. `register_job`: generates a `job_id` (UUID). If the same `file_path` + `file_generation` already has a SUCCESS row, it inserts `DUPLICATE_SKIPPED` and stops. Otherwise it inserts an `IN_PROGRESS` row into `job_metadata` (DML via the BigQuery connector).
   4. `acquire_slot`: waits for a free Dataflow slot. A slot is a lock object `gs://<dataflow bucket>/locks/dataflow-slot-<n>`, created with `ifGenerationMatch=0`, so only one execution can own it. Busy executions check again every 60 s. A lock older than 2 hours is treated as abandoned and removed.
   5. `launch_dataflow`: calls `flexTemplates.launch` with `input_file`, `audit_job_id`, `file_generation`, `pubsub_message_id` and `trigger_type=PUBSUB`.
   6. `wait_for_dataflow`: polls `jobs.get` every 60 s until the job reaches a terminal state (DONE / FAILED / CANCELLED), with a 1-hour limit.
   7. `release_slot`: deletes the lock object, whether the job succeeded, failed or never launched, so the next queued file can start.
   8. `finalize`: reads the status that Dataflow wrote. If the job did not finish DONE, or the status is still `IN_PROGRESS`, it sets the status to `FAILED`. It moves the file to `archive/YYYY/MM/DD/` (SUCCESS) or `invalid/YYYY/MM/DD/` (anything else), records `final_file_path`, and fails the execution if the status is `FAILED`.
   9. `publish_failure` (only when the final status is FAILED or REJECTED, and before the execution fails): publishes one error event to topic `dfa-ingest-errors` (section 7a). A failure to publish is logged but doesn't change the job outcome.
5. **Dataflow job** validates the file, loads valid files, reconciles counts and writes the audit records (section 5).

## 4. Repository layout
```
Makefile                 make targets (section 8)
project.md               this document
infra/                   Terraform: GCS, Pub/Sub, Eventarc, Workflows, BQ, IAM, Artifact Registry
  bq/*.json              BQ table schemas (job_metadata, validation_metadata, customer, user, orders)
workflows/
  ingest_file.yaml       per-file orchestration (deployed by Terraform)
dataflow-job/            Beam pipeline (Flex Template)
  main.py, setup.py, requirements.txt, Dockerfile, metadata.json
  contracts/*.yaml       per-entity contract: name pattern, delimiter, columns/types, target table
  pipeline/              options, contracts, checks, transforms/, pipeline.py
  tests/                 TestPipeline / assert_that unit tests
scripts/gcs_check.sh     resource health check (make gcs-check)
mockdata/                sample good/bad files + generate.py
```

## 5. Dataflow pipeline (Apache Beam)
Built from Beam transforms only: `Create`, `ReadFromText`, `ParDo` with tagged outputs, side inputs, `CombineGlobally`, `Flatten` and `WriteToBigQuery`. The driver does no record processing.

| Step | Transform | Purpose |
|---|---|---|
| 1 | `ValidateFileStructure` (DoFn) | Reads only the first and last lines. Contract match, file name, header and trailer-format checks. Emits `FileMeta` as a side input. |
| 2 | `ReadFromText(skip_header_lines=1)` → filter trailer → `ParseRecord` | Typed rows plus tagged `delimiter_error` / `type_error` rows. Adds `_job_id`, `_source_file`, `_loaded_at`. |
| 3 | Count / sample combiners | Delimiter, data-type and trailer-count checks |
| 4 | Gate (`file_ok` side input) → `WriteToBigQuery(FILE_LOADS)` | Loads only when there is no ERROR failure |
| 5 | `ReconcileCounts` | Waits for the load jobs, then compares file rows, trailer count and `COUNT(*) WHERE _job_id` in BQ |
| 6 | Audit | All check results go to `validation_metadata`. A `MERGE` on `job_metadata` writes the final status and counts. |

Pipeline options: `--input_file`, `--audit_job_id` (required unless `trigger_type=MANUAL`; it was originally `--job_id`, which did not reach the pipeline through the flex template launch), `--file_generation`, `--pubsub_message_id`, `--trigger_type`, `--audit_dataset`, `--target_dataset`.

## 6. Validation checks
All checks have ERROR severity. Each one writes a row to `file_audit.validation_metadata`.

| check_name | check_type | Stage | Rule |
|---|---|---|---|
| `contract_match` | CONTRACT_MATCH | PRE_LOAD | The file-name prefix maps to a contract (`customer`, `user`, `orders`) |
| `file_name` | FILE_FORMAT | PRE_LOAD | Matches `^<entity>_\d{8}\.csv$`, and the date is a real calendar date |
| `delimiter` | FILE_FORMAT | PRE_LOAD | The header splits on the contract delimiter into the expected column count, and every data row has the same field count. The detected delimiter is recorded. |
| `header` | FILE_FORMAT | PRE_LOAD | Header columns exactly equal the contract columns, in the same order |
| `trailer` | FILE_FORMAT | PRE_LOAD | The last line is `T|<count>|<YYYYMMDD>`, the date equals the file date, and the count equals the data rows |
| `data_type` | DATA_CONTRACT | PRE_LOAD | Non-null and type parse for each column (INTEGER, NUMERIC, DATE (with format), TIMESTAMP, BOOLEAN) |
| `recon_row_count` | RECONCILIATION | POST_LOAD | Data rows in the file = trailer count = rows in BQ for this `job_id` (SKIPPED if the file was rejected) |

`validation_result`: PASS / FAIL / SKIPPED. Failing checks include expected vs actual values and up to 10 sample rows in `details`.

## 7. BigQuery tables
| Table | Purpose |
|---|---|
| `file_audit.job_metadata` | One row per processing attempt. It holds file info, the Pub/Sub ids, status, row counts, validation/recon status and errors. `service_name='workflows'` with `service_revision`=the workflow execution id, so each row links to its execution. |
| `file_audit.validation_metadata` | One row per check per job (section 6) |
| `raw.customer`, `raw.user`, `raw.orders` | Target tables, each with the lineage columns `_job_id`, `_source_file` and `_loaded_at` |

Schemas: `infra/bq/*.json`. Tables are day-partitioned (audit tables on `created_at`, target tables on `_loaded_at`).

## 7b. Incident agent (df-agent)
A LangGraph multi-agent system consumes `dfa-ingest-errors-sub`. It investigates each failed or rejected job, auto-retries transient failures (create-only copy `invalid/` → `landing/`, max 2/day), and writes incident reports to `df-agent/actions/<job_id>/`. It runs as the least-privilege SA `dfa-agent` and records every run in `file_audit.agent_incidents`. Design and usage: [df-agent/df-agent.md](df-agent/df-agent.md). Make targets: `agent-venv`, `agent-test`, `agent-listen`, `agent-once`, `agent-replay`, `agent-approve`, `agent-brain-sync`.

## 7a. Error events (topic `dfa-ingest-errors`)
One message per failed or rejected job. It's meant to be self-contained, so an automated investigator (e.g. an AI agent) can triage the job without first querying several systems. The subscription `dfa-ingest-errors-sub` keeps messages for 7 days until a consumer exists. `make errors` peeks at them without acking.

**Attributes**, for subscription filters: `status` (FAILED / REJECTED), `error_code`, `job_id`, `job_name`, `file_name`.

**Data** (JSON, `schema_version: "1"`):
| Field | Content |
|---|---|
| `event_type` | `ingestion.job.failed` or `ingestion.job.rejected` |
| `job` | `job_id`, `status`, `status_reason`, `error_code`, `error_message`, `job_name`, `contract_name`, `target_table`, `rows_in_file`, `rows_loaded`, `validation_status`, `recon_status`, `start_time`, `end_time`, `attempt_number`, read from `job_metadata` |
| `file` | `bucket`, `name`, `generation`, `original_path`, `final_path` (where the file was moved: `invalid/…`) |
| `failed_checks[]` | per failed check: `check_name`, `check_type`, `summary`, `failed_rules`, `details_json` (expected vs actual and sample rows, truncated to 2,000 chars), read from `validation_metadata` |
| `dataflow` | `job_id`, `state`, `launch_error` (if the launch or polling failed), `error_messages[]` (up to 5 Dataflow job errors), `console_url` |
| `orchestration` | `workflow`, `execution_id`, `console_url` |
| `audit_queries` | ready-to-run SQL for this job's `job_metadata` and `validation_metadata` rows |
| `retry_hint` | REJECTED: fix the file and re-upload it to `landing/`. FAILED: check the Dataflow errors, then re-upload the same file to retry. |

**Job status lifecycle:** `IN_PROGRESS` → `SUCCESS` | `REJECTED` (validation failed) | `FAILED` (pipeline/recon error) | `DUPLICATE_SKIPPED`.

## 8. Make targets
| Target | Action |
|---|---|
| `make plan` | `terraform init` + `plan` |
| `make gcs-setup` | `terraform apply` only (all resources, including the workflow). It does not build the image. |
| `make docker` | Builds the Flex Template image locally (`linux/amd64`), pushes it to Artifact Registry, then runs `make template` |
| `make template` | Writes the Flex Template spec to GCS, pointing at the pushed image. Rerun after changing `metadata.json`. |
| `make gcs-check` | Read-only health check. Confirms every resource exists and is active: APIs, buckets and the bucket notification, Pub/Sub, Eventarc trigger `ACTIVE`, workflow `ACTIVE`, BQ datasets and tables, service accounts, AR repo. The flex template image/spec only produces warnings. Exits 1 if anything fails. |
| `make gcs-destroy` | `terraform destroy`. Buckets and datasets are force-deleted. |
| `make run-dataflow FILE=…` | Launch the Dataflow job from local (`trigger_type=MANUAL`) |
| `make run-local FILE=…` | Run with DirectRunner as a smoke test |
| `make upload-mock` | Copy mock files to `landing/` (triggers the full flow) |
| `make executions` | List recent workflow executions and their states |
| `make errors` | Peek at up to 10 error events on `dfa-ingest-errors-sub` (not acked, so they stay for the future agent) |
| `make bq-clean-up` | `TRUNCATE` every table: `file_audit.job_metadata`, `file_audit.validation_metadata`, and all `raw.*` target tables. It lists the tables and asks you to type `yes`; `CONFIRM=yes` skips the prompt. Tables and schemas are kept. Only rows are deleted. |
| `make venv` / `make test` / `make mockdata` | Python 3.12 venv, pytest, regenerate mock files |

## 9. GCP resources (Terraform)
- **APIs:** dataflow, workflows, workflowexecutions, eventarc, pubsub, bigquery, storage, artifactregistry, cloudbuild, iam, compute
- **GCS:** `<project>-dfa-landing` (`landing/`, `archive/`, `invalid/`, `manual/`) and `<project>-dfa-dataflow` (temp, staging, templates)
- **Pub/Sub:** topic `dfa-file-landed` with the bucket notification. Eventarc creates and owns the push subscription. Error topic `dfa-ingest-errors` with pull subscription `dfa-ingest-errors-sub` (7-day retention). The workflow SA can publish to it.
- **Eventarc:** trigger `dfa-file-landed` (Pub/Sub topic → workflow)
- **Workflows:** `dfa-ingest-file`, with Terraform passing the config as user env vars
- **BigQuery:** datasets `file_audit`, `raw` and their tables
- **IAM:** service accounts `dfa-dataflow` (Dataflow workers) and `dfa-workflow` (Eventarc trigger + workflow identity; `actAs` dfa-dataflow)
- **Artifact Registry:** `dfa-dataflow` (Flex Template image)

## 10. Mock data
| File | Expected |
|---|---|
| `customer_20261015.csv`, `user_20261015.csv`, `orders_20261015.csv`, `customer_20261017.csv` | SUCCESS |
| `customer_20261016.csv` | REJECTED – data_type (NOT_NULL / TYPE errors) |
| `user_20261016.csv` | REJECTED – header (missing `role`) |
| `unknown_20261015.csv` | REJECTED – contract_match |
| `customer_id_200.csv`, `customer_2026-10-19.csv`, `orders_20261016.xlsx` | REJECTED – file_name |
| `customer_20261018.csv` | REJECTED – trailer (count 7 vs 5 rows) |
| `orders_20261018.csv` (pipe-delimited) | REJECTED – delimiter + header |
| `user_20261020.csv` | REJECTED – trailer (missing) |
| `customer_20261021.csv` (Latin-1 encoded) | **FAILED**: the Dataflow job crashes with `UnicodeDecodeError` → df-agent CODE_BUG incident |
| any 4 valid files uploaded together with `MAX_JOBS=4` | some **FAILED** with `QUOTA_EXCEEDED` → df-agent auto-retries them (TRANSIENT_INFRA) |

`generate.py` appends `T|<count>|<YYYYMMDD>` to every CSV. Run `make mockdata` to regenerate the files; the committed CSVs predate the trailers. `customer_id_200.csv` is hand-made and not generated.

## 11. Testing / runbook
```bash
make venv            # .venv already exists; this installs any missing dev deps
make mockdata        # regenerate CSVs with trailers
make test            # unit tests, no GCP needed
make plan
make gcs-setup       # ~5 min, infra only
make docker          # build + push the image, write the template spec (needs Docker running)
make gcs-check       # everything should be OK
make run-dataflow FILE=customer_20261015.csv   # manual run; uploads to manual/ so no trigger
make upload-mock     # full flow: Pub/Sub -> Eventarc -> Workflows -> Dataflow
make executions      # watch workflow executions (or the Workflows console)
make gcs-destroy
```
Verify in BigQuery:
```sql
SELECT file_name, status, rows_in_file, rows_loaded, recon_status, final_file_path
FROM file_audit.job_metadata ORDER BY created_at DESC;
SELECT file_name, check_name, validation_result, summary
FROM file_audit.validation_metadata ORDER BY created_at DESC;
```

## 12. Cost
| Item | Cost at test volume |
|---|---|
| Workflows | 5,000 steps a month free (~45 steps per file), then $0.01 per 1,000 steps |
| Eventarc / Pub/Sub | Free tier |
| Dataflow | ~$0.02 per file (one small worker for ~5–8 min, including startup) |
| Artifact Registry, GCS, BigQuery | ~$0, within free tiers (the image is built locally, not in Cloud Build) |
| **2-hour test session** | **~$0.25–0.50**. Nothing costs money while idle, so leaving it deployed is ~$0. |

All buckets use `force_destroy` and the datasets use `delete_contents_on_destroy`, so `make gcs-destroy` removes everything except the enabled APIs.

## 13. Status
| Item | Status |
|---|---|
| Design (this doc) | Done (Workflows) |
| `infra/` Terraform | Switched to Workflows + Eventarc. Composer, the Airflow subscription and the DLQ are removed. Not applied. |
| `workflows/ingest_file.yaml` | Written. Not deployed. |
| Dataflow job (`dataflow-job/`) | Written, with unit tests. Not yet run. The MERGE no longer overwrites `service_name`. |
| `dags/`, `infra/composer.tf` | Obsolete. Delete them with `rm -rf dags infra/composer.tf`. `composer.tf` is now just a comment, so Terraform works either way. |
| Makefile / `gcs_check.sh` | Updated: `dags-deploy` removed, `executions` added, and the health check covers the Eventarc trigger and workflow |
| Mock data (`generate.py`) | Updated. Run `make mockdata` to regenerate. |

## 14. Implementation notes
- **Load sequencing:** `WriteToBigQuery(FILE_LOADS)` uses dynamic destinations (the table comes from the contract side input), so Beam loads through temp tables plus copy jobs. `ReconcileCounts` takes the load and copy job-id pairs as side inputs and waits on each job before counting.
- **Audit writes:** `validation_metadata` uses streaming inserts, with JSON columns sent as JSON strings. `job_metadata` is only ever written with DML (workflow INSERT/UPDATE, Dataflow MERGE), so rows never sit in the streaming buffer and can always be updated.
- **Manual runs:** `make run-dataflow` / `run-local` skip the workflow. The pipeline's MERGE inserts the job_metadata row (`trigger_type=MANUAL`). The file is not moved, because only the workflow does that.
- **Retries:** Eventarc retries if the workflow execution cannot be created. Inside the workflow, connector calls use the default retry policy. Duplicate deliveries of the same object generation are caught by the `DUPLICATE_SKIPPED` check.

- **Workflow API calls:** the workflow calls the Dataflow, BigQuery and GCS REST APIs with `http.*` + OAuth2 rather than connectors. That keeps launch non-blocking (we poll ourselves) and makes every request explicit. BigQuery parameters are all STRING and are cast in SQL. Empty string means NULL (`NULLIF`).
- **Queueing:** a file's `job_metadata` row is `IN_PROGRESS` from registration, so `duration_seconds` includes time spent waiting for a slot. A bulk drop of the ~13 mock files takes roughly 13 × 6 min ≈ 75 min end to end. Raise `max_concurrent_jobs` (Terraform variable) only after raising the `IN_USE_ADDRESSES` quota: each job needs about 2 IPs. `make run-dataflow` bypasses the slots, so don't run it while a bulk upload is being processed.
- **Re-processing a file:** upload it again. A new generation gets a new job, and the same generation is skipped as `DUPLICATE_SKIPPED` once it has succeeded.

## 15. Open items / things to check on first run
- `make docker` needs Docker Desktop running locally. Your gcloud user needs `roles/artifactregistry.writer`; project Owner/Editor already has it. On Apple Silicon the `linux/amd64` build is emulated, so the first build is slow (several minutes).
- On a brand-new project, the Eventarc and Pub/Sub service agents are sometimes created a minute or two after the APIs are enabled. If `terraform apply` fails on the trigger or the token-creator binding, rerun `make gcs-setup`; it continues from where it stopped.
- Data-quality rules beyond types (regex, allowed values, uniqueness) are not implemented yet. They can be added as extra per-column rules in the contracts.
