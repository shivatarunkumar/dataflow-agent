# GCS → BigQuery Ingestion (Airflow on Cloud Composer 3 + Dataflow)

> **Status (2026-11-03): implemented, not yet deployed or tested.** The design is approved and the code in section 16
> is written. GCP still runs the Eventarc + Cloud Workflows version (git `eb185bc`) until `make gcs-setup` is applied.

## 1. Overview
Files dropped into a GCS landing bucket are validated and loaded into BigQuery by an Apache Beam (Python) Dataflow
job. **Apache Airflow on Cloud Composer 3** orchestrates the flow. A GCS → Pub/Sub notification puts one message per
file on a pull subscription. An Airflow listener DAG pulls it and starts one **ingestion DAG run per file**. That run
registers the job, launches the Dataflow Flex Template, waits for it, then finalizes the audit record and moves the
file. Every run and every validation check is recorded in BigQuery audit tables. Every failure publishes one error
event, which the df-agent investigates **from the Airflow DAG run down to the Dataflow step** (section 15).

```
 ┌──────────────┐ OBJECT_FINALIZE ┌──────────────────────┐  pull (every 2 min)  ┌──────────────────────────────┐
 │ GCS landing/ │ ──────────────▶ │ Pub/Sub dfa-file-    │ ───────────────────▶ │ DAG pubsub_file_listener     │
 └──────────────┘                 │ landed  → sub        │                      │ 1 trigger per file generation│
                                  │ dfa-file-landed-     │                      └──────────────┬───────────────┘
                                  │ airflow (7-day keep) │                                     │ TriggerDagRun
                                  └──────────────────────┘                      ┌──────────────▼───────────────┐
                                                                                │ DAG gcs_to_bq_dataflow       │
                                                                                │ register → run_dataflow →    │
                                                                                │ finalize (audit, move, event)│
                                                                                └──────────────┬───────────────┘
                                                                                               │ Flex Template
                                                                                ┌──────────────▼───────────────┐
                                                                                │ Dataflow (Beam Python)       │
                                                                                │ validate → load → recon      │
                                                                                └──────┬────────────────┬──────┘
                                                                         raw.<table> ◀─┘                └─▶ file_audit.*
            FAILED / REJECTED ─▶ Pub/Sub dfa-ingest-errors ─▶ df-agent (Airflow → Dataflow → BigQuery investigation)
```

## 2. Key decisions
| Topic | Decision |
|---|---|
| GCP project | `bigquerytarun` (`PROJECT_ID` default in the Makefile) |
| Region | `europe-west2` |
| Orchestration | **Airflow on Cloud Composer 3** (smallest size). It **replaces** Eventarc + Cloud Workflows; those resources are removed. |
| Airflow version | Composer image `composer-3-airflow-2.10.5` (Terraform variable `composer_image`; the latest build of that Airflow version is used). DAGs use Airflow 2 APIs that also exist in Airflow 3 where possible. |
| Composer lifecycle | **Off by default** (`enable_composer = false`), because it costs ~$20/day even when idle. `make airflow-up` creates it for a test session and `make airflow-down` deletes **only** the Composer environment. Buckets, BigQuery, Pub/Sub and the agent stay. |
| Events while Airflow is down | Not lost. GCS notifications queue on subscription `dfa-file-landed-airflow` (7-day retention) and are processed when the environment is next up. |
| One run per file | The listener triggers the ingestion DAG with a **deterministic `run_id`** built from the object generation and name. A redelivered Pub/Sub message maps to the same `run_id`, and Airflow refuses a duplicate run (`skip_when_already_exists`). This replaces the GCS "claim" objects used by Workflows. |
| Concurrency | **One Dataflow job at a time.** `run_dataflow` has `max_active_tis_per_dag = DFA_MAX_CONCURRENT_JOBS` (default 1), and `maxWorkers = 1`. Other files wait in Airflow as *scheduled* task instances. This replaces the GCS lock-object semaphore. Composer 3 runs in a Google-managed tenant project and uses **no external IPs** in ours, so the 4-IP quota is still only shared by Dataflow (2 IPs per job). |
| File format / naming / trailer / failure handling / load mode | Unchanged: CSV only, `<entity>_<YYYYMMDD>.csv`, trailer `T|<count>|<YYYYMMDD>`, any ERROR check rejects the whole file, append only. |
| Dataflow packaging | Flex Template (image in Artifact Registry, spec in GCS). The launcher base image is pinned with `BASE_IMAGE` in the Makefile/Dockerfile: Google's `python312-template-launcher-base` releases from Oct–Nov 2026 fail to unpack (`archive/tar: invalid tar header`), see section 14. |
| IaC | Terraform in `infra/`, local state. DAGs are copied to the Composer DAG bucket by `make dags-deploy`. |

**Why Composer now:** the goal is to run the pipeline the way it's typically run in production (Airflow) and to have
the agent investigate Airflow as well as Dataflow. The cost is controlled by turning the environment on only for
test sessions (section 13).

## 3. End-to-end flow
1. **File lands** at `gs://<project>-dfa-landing/landing/<file>`.
2. **GCS notification** (OBJECT_FINALIZE, prefix `landing/`) publishes to topic `dfa-file-landed`.
3. **Subscription `dfa-file-landed-airflow`** (pull, ack deadline 60 s, 7-day retention) holds the message.
4. **DAG `pubsub_file_listener`** (`dags/pubsub_file_listener.py`, every 2 min, `max_active_runs=1`):
   1. `pull_file_notifications` (`PubSubPullSensor`, up to 10 messages, acked when pulled) drops anything that is not
      `OBJECT_FINALIZE` under `landing/`, as well as folder markers.
   2. `trigger_ingestion` (mapped `TriggerDagRunOperator`, one per file) starts `gcs_to_bq_dataflow` with
      `run_id = file__<generation>__<sanitised name>` and conf
      `{bucket, name, generation, size, time_created, message_id, subscription, delivery_attempt}`.
      An existing `run_id` (duplicate delivery) is skipped, not failed.
5. **DAG `gcs_to_bq_dataflow`** (`dags/gcs_to_bq_dataflow.py`, `schedule=None`, `dagrun_timeout=3h`):
   1. `register_job`: generates a `job_id` (UUID). If the same `file_path` + `file_generation` already has a SUCCESS
      row, it inserts `DUPLICATE_SKIPPED`. Otherwise it inserts an `IN_PROGRESS` row into `job_metadata`, with
      `service_name='composer'` and `service_revision=<dag_run.run_id>` so every row links back to its DAG run.
   2. `is_new_file` (short-circuit): stops the run for duplicates.
   3. `run_dataflow` (`DataflowStartFlexTemplateOperator`, `wait_until_finished=True`, `execution_timeout=60 min`,
      `max_active_tis_per_dag=DFA_MAX_CONCURRENT_JOBS`, no Airflow retries): launches job `ingest-<job_id>` with
      `input_file`, `audit_job_id`, `file_generation`, `pubsub_message_id`, `trigger_type=PUBSUB` and label
      `audit_job_id=<job_id>`. On timeout Airflow cancels the Dataflow job (`on_kill`).
   4. `finalize` (`trigger_rule=all_done`):
      - Finds the Dataflow job by its name `ingest-<job_id>` and reads its final state, so it also works when
        `run_dataflow` failed.
      - Reads the status that Dataflow wrote. If the job did not finish `JOB_STATE_DONE`, or the status is still
        `IN_PROGRESS`, it sets `FAILED` with `error_code = DATAFLOW_JOB_FAILED` (or `AIRFLOW_TASK_FAILED` when no
        Dataflow job was ever created, e.g. a launch error). The `error_message` keeps the existing form
        `Dataflow job <id> ended in <state>. <airflow error>` so replays can recover the Dataflow job id.
      - Moves the file to `archive/YYYY/MM/DD/` (SUCCESS) or `invalid/YYYY/MM/DD/` (anything else) and records
        `final_file_path`.
      - For FAILED or REJECTED, publishes one error event to `dfa-ingest-errors` (section 7a, schema v2). A publish
        failure is logged but doesn't change the outcome.
      - Fails the task (and so the DAG run) if the status is `FAILED`. A REJECTED file is a data problem, not an
        Airflow failure, so the run stays green.
6. **Dataflow job** validates the file, loads valid files, reconciles counts and writes the audit records (section 5).

## 4. Repository layout
```
Makefile                 make targets (section 8)
project.md               this document
infra/                   Terraform: GCS, Pub/Sub, Composer 3, BQ, IAM, Artifact Registry, df-agent
  composer.tf            Composer 3 environment (count = enable_composer) + its service account
  bq/*.json              BQ table schemas
dags/                    Airflow DAGs (copied to the Composer DAG bucket by make dags-deploy)
  pubsub_file_listener.py
  gcs_to_bq_dataflow.py
  dfa_common/            config (env vars set by Terraform), BigQuery/GCS/Dataflow helpers, error-event builder
  tests/                 DAG import + unit tests (no Airflow server, no GCP)
dataflow-job/            Beam pipeline (Flex Template).
df-agent/                incident agent (section 15, df-agent/df-agent.md)
scripts/gcs_check.sh     resource health check (make gcs-check)
mockdata/                sample good/bad files + generate.py
```
Removed: `workflows/ingest_file.yaml`, `infra/workflows.tf`.

## 5. Dataflow pipeline (Apache Beam)
Unchanged. Built from Beam transforms only: `Create`, `ReadFromText`, `ParDo` with tagged outputs, side inputs,
`CombineGlobally`, `Flatten` and `WriteToBigQuery`.

| Step | Transform | Purpose |
|---|---|---|
| 1 | `ValidateFileStructure` (DoFn) | Reads only the first and last lines. Contract match, file name, header and trailer-format checks. Emits `FileMeta` as a side input. |
| 2 | `ReadFromText(skip_header_lines=1)` → filter trailer → `ParseRecord` | Typed rows plus tagged `delimiter_error` / `type_error` rows. Adds `_job_id`, `_source_file`, `_loaded_at`. |
| 3 | Count / sample combiners | Delimiter, data-type and trailer-count checks |
| 4 | Gate (`file_ok` side input) → `WriteToBigQuery(FILE_LOADS)` | Loads only when there is no ERROR failure |
| 5 | `ReconcileCounts` | Waits for the load jobs, then compares file rows, trailer count and `COUNT(*) WHERE _job_id` in BQ |
| 6 | Audit | All check results go to `validation_metadata`. A `MERGE` on `job_metadata` writes the final status and counts. |

Pipeline options: `--input_file`, `--audit_job_id`, `--file_generation`, `--pubsub_message_id`, `--trigger_type`,
`--audit_dataset`, `--target_dataset`. (The old DAG passed `job_id`, which never reached the pipeline; the new DAG
passes `audit_job_id`.)

## 6. Validation checks
Unchanged. All have ERROR severity and write one row each to `file_audit.validation_metadata`.

| check_name | check_type | Stage | Rule |
|---|---|---|---|
| `contract_match` | CONTRACT_MATCH | PRE_LOAD | The file-name prefix maps to a contract (`customer`, `user`, `orders`) |
| `file_name` | FILE_FORMAT | PRE_LOAD | Matches `^<entity>_\d{8}\.csv$`, and the date is a real calendar date |
| `delimiter` | FILE_FORMAT | PRE_LOAD | Header and every data row split into the expected field count |
| `header` | FILE_FORMAT | PRE_LOAD | Header columns exactly equal the contract columns, in order |
| `trailer` | FILE_FORMAT | PRE_LOAD | Last line `T|<count>|<YYYYMMDD>`; date = file date; count = data rows |
| `data_type` | DATA_CONTRACT | PRE_LOAD | Non-null and type parse per column |
| `recon_row_count` | RECONCILIATION | POST_LOAD | File rows = trailer count = rows in BQ for this `job_id` |

## 7. BigQuery tables
| Table | Purpose |
|---|---|
| `file_audit.job_metadata` | One row per processing attempt. `service_name='composer'`, `service_revision=<dag_run.run_id>` (DAG id is always `gcs_to_bq_dataflow`). No schema change. |
| `file_audit.validation_metadata` | One row per check per job |
| `file_audit.agent_incidents` | df-agent runs (append-only) |
| `raw.customer`, `raw.user`, `raw.orders` | Target tables with lineage columns `_job_id`, `_source_file`, `_loaded_at` |

## 7a. Error events (topic `dfa-ingest-errors`, schema v2)
One message per FAILED or REJECTED job, self-contained so the agent can start without querying several systems.
Subscription `dfa-ingest-errors-sub` keeps messages for 7 days. `make errors` peeks without acking.

**Attributes:** `status`, `error_code`, `job_id`, `job_name`, `file_name`, plus new `orchestrator=airflow`.

**Data** (JSON, `schema_version: "2"`). `job`, `file`, `failed_checks[]`, `dataflow`, `audit_queries` and
`retry_hint` are the same as v1. `orchestration` changes from Workflows fields to Airflow fields:

| Field | Content |
|---|---|
| `orchestration.type` | `airflow` |
| `orchestration.environment` | Composer environment name (`dfa-composer`) |
| `orchestration.dag_id` / `dag_run_id` | `gcs_to_bq_dataflow` / `file__<generation>__<name>` |
| `orchestration.tasks[]` | every task instance of the run at finalize time: `task_id`, `state`, `try_number`, `start_date`, `end_date`, `duration` |
| `orchestration.failed_task` | the first failed task (usually `run_dataflow`), or empty |
| `orchestration.task_error` | tail (2,500 chars) of the failed task's log: the Airflow-side exception, e.g. a launch `HttpError 400`, `AirflowTaskTimeout`, or the Dataflow-hook failure message |
| `orchestration.console_url` | Airflow UI link to the DAG run |
| `dataflow.job_id` | found by job name `ingest-<job_id>`; empty when the launch never created a job |

The agent still accepts v1 (Workflows) events, so old incidents can be replayed.

**Job status lifecycle:** `IN_PROGRESS` → `SUCCESS` | `REJECTED` | `FAILED` | `DUPLICATE_SKIPPED`.

## 8. Make targets
| Target | Action |
|---|---|
| `make plan` / `make gcs-setup` | `terraform plan` / `apply` of everything. Composer **keeps its current state** (up stays up, down stays down); `COMPOSER=true|false` overrides. |
| `make airflow-up` | `terraform apply -var enable_composer=true` (~25 min), then `make dags-deploy` |
| `make airflow-down` | `terraform apply -var enable_composer=false`: deletes only the Composer environment (~10 min). **Run this at the end of every session.** |
| `make dags-deploy` | `gcloud storage rsync dags/` → the environment's DAG folder (excluding `tests/`) |
| `make airflow-ui` | Prints the Airflow UI URL |
| `make airflow-runs` | Lists recent `gcs_to_bq_dataflow` DAG runs and their states (`gcloud composer environments run … dags list-runs`) |
| `make airflow-admin` | Gives your account the Airflow Admin role (sign in to the UI once first; new users get Viewer) |
| `make airflow-test-launch-failure FILE=…` | Uploads the file to `manual/` (no notification) and triggers `gcs_to_bq_dataflow` with a missing template path: a FAILED job before any Dataflow job exists |
| `make dags-test` | DAG import + unit tests locally (no Composer needed) |
| `make docker` / `make template` | Build + push the Flex Template image (base image: `BASE_IMAGE`) / write the spec. |
| `make gcs-check` | Health check. Now also checks the file-landed subscription and, when it exists, that the Composer environment is `RUNNING` and both DAGs are present in its bucket. No Eventarc/Workflows checks. |
| `make run-dataflow FILE=…` / `make run-local FILE=…` | Manual runs that bypass Airflow (unchanged) |
| `make upload-mock` | Copy mock files to `landing/` (triggers the full flow) |
| `make errors` / `make bq-clean-up` / `make gcs-destroy` | Unchanged |
| removed | `make executions` (Workflows) |

## 9. GCP resources (Terraform)
- **APIs:** add `composer.googleapis.com`. Eventarc/Workflows APIs are removed from the list (left enabled in the project, as with every API here).
- **GCS:** `<project>-dfa-landing` and `<project>-dfa-dataflow`. The `claims/` lifecycle rule goes (no claims any more). Composer creates its own environment bucket (DAGs, logs).
- **Pub/Sub:** topic `dfa-file-landed` + bucket notification; **new pull subscription `dfa-file-landed-airflow`** (ack 60 s, 7-day retention, no expiry). Error topic `dfa-ingest-errors` + `dfa-ingest-errors-sub` unchanged.
- **Composer 3:** environment `dfa-composer` in europe-west2, `ENVIRONMENT_SIZE_SMALL`, scheduler/web server/worker at 0.5 vCPU each, 1–2 workers, no triggerer. Created only when `enable_composer = true`. Airflow env vars `DFA_*` (project, region, buckets, template path, datasets, Dataflow SA, subscription, error topic, max concurrent jobs, environment name). Airflow config override `webserver-rbac_user_registration_role = Viewer`, so principals that only read (the agent) get Airflow **Viewer**.
- **Service account `dfa-composer`** (replaces `dfa-workflow`): `composer.worker`, `dataflow.developer`, `bigquery.dataEditor`, `bigquery.jobUser`, `logging.logWriter`; `storage.objectAdmin` on both buckets; `pubsub.subscriber` on `dfa-file-landed-airflow`; `pubsub.publisher` on `dfa-ingest-errors`; `iam.serviceAccountUser` on `dfa-dataflow`. The Composer service agent gets `roles/composer.ServiceAgentV2Ext` on that SA (required for Composer 3).
- **Removed:** Eventarc trigger, workflow `dfa-ingest-file`, SA `dfa-workflow`, the Pub/Sub token-creator binding.
- **df-agent SA `dfa-agent`:** add `roles/composer.user` (read the environment and call the Airflow REST API) and drop `workflows.viewer`.
- **BigQuery, Artifact Registry, `dfa-dataflow` SA:** unchanged.

## 10. Mock data
Unchanged files and expectations. The two agent scenarios now run through Airflow:

| File | Expected |
|---|---|
| `customer_20261015.csv`, `user_20261015.csv`, `orders_20261015.csv`, `customer_20261017.csv` | SUCCESS (DAG run green) |
| `customer_20261016.csv`, `user_20261016.csv`, `unknown_20261015.csv`, `customer_id_200.csv`, `customer_2026-10-19.csv`, `orders_20261016.xlsx`, `customer_20261018.csv`, `orders_20261018.csv`, `user_20261020.csv` | REJECTED (DAG run green, error event published) |
| `customer_20261021.csv` (Latin-1) | **FAILED**: `run_dataflow` fails because Dataflow crashed with `UnicodeDecodeError` → agent path *Airflow `run_dataflow` failed → Dataflow job FAILED → step ParseRecord → UnicodeDecodeError* → CODE_BUG |
| 4 valid files with `MAX_JOBS=4` | some **FAILED** with `QUOTA_EXCEEDED` → TRANSIENT_INFRA → auto-retry |
| **new:** `make airflow-test-launch-failure FILE=customer_20261015.csv` | **FAILED** before Dataflow: the `run_dataflow` launch is rejected, no Dataflow job → agent stops at the AIRFLOW layer → CONFIG (`dataflow_launch_rejected` or `template_missing`) |

## 11. Testing / runbook
```bash
make dags-test                 # DAG import + unit tests (local, no GCP)
make test                      # pipeline unit tests
make agent-test                # agent tests (incl. new Airflow tools and v2 events)
make plan && make gcs-setup    # everything except Composer
make docker                    # only if the image/spec changed
make airflow-up                # ~25 min: Composer env + DAG upload   ← billing starts
make airflow-ui                # open the UI, unpause both DAGs if needed
make upload-mock               # full flow
make airflow-runs              # watch DAG runs (or the Airflow UI)
make agent-listen              # investigate failures as they arrive
make airflow-down              # ~10 min                               ← billing stops
```

## 12. Cost
| Item | Cost |
|---|---|
| **Composer 3 (small)** | **~$0.80–1.00/hour while the environment exists**, idle or not (~$20/day). A 3-hour session is ~3h 40m billed (create + use + delete) ≈ **$3–5**. |
| Dataflow | ~$0.02 per file |
| Pub/Sub, GCS, BigQuery, Artifact Registry | ~$0 (free tiers) |
| With Composer down | ~$0. Events wait on the subscription for up to 7 days. |

**Guard rails:** Composer is off unless `enable_composer = true`; `make airflow-down` ends every session; and a
billing budget alert (e.g. £5) is recommended (command in section 14).

## 13. Agent
The df-agent design is unchanged apart from the Airflow investigation in section 15. Full details will be in
`df-agent/df-agent.md` §20.

## 14. Open items / things to check on first run
- Confirm `composer-3-airflow-2.10.5` is still offered in europe-west2 (Composer "Version list" docs page, or the version picker in the console's Create environment form). Override with `-var composer_image=…`; switching to an Airflow 3 image needs a DAG review first.
- The first `make airflow-up` on a project can fail while the Composer service agent is being created. Rerun it.
- The Airflow UI uses Google sign-in. With the registration role set to Viewer, give your own account Admin once after first logging in: `make airflow-admin` (`gcloud composer environments run dfa-composer … users add-role -- -e <you> -r Admin`).
- Budget alert: `gcloud billing budgets create --billing-account=<ID> --display-name=dfa-guard --budget-amount=5GBP --threshold-rule=percent=0.5 --threshold-rule=percent=1.0`.

## 15. Agent investigation: Airflow → Dataflow → BigQuery
The agent follows the same path an on-call engineer would: it starts at the Airflow DAG run, finds the task that
failed, reads that task's log, and only then goes into Dataflow (job, failing step, worker errors) and BigQuery
(audit rows, failed checks). It reports the **failing layer** and the full path.

**New GCP tools** (typed, read-only GETs on the Airflow REST API `…/api/v1`, reached through the Composer
environment's `airflowUri` with the impersonated `dfa-agent` token):

| Tool | Returns | Signal? |
|---|---|---|
| `get_airflow_dag_run(dag_run_id)` | state, start/end, duration, conf (bucket/name/generation) | context |
| `get_airflow_task_instances(dag_run_id)` | each task's state, try number, duration, operator, queued time (shows time spent waiting for the Dataflow slot) | context |
| `get_airflow_task_log(dag_run_id, task_id, try_number)` | the task log; head + tail kept, tail holds the exception | **signal** |
| `find_dataflow_job(audit_job_id)` | Dataflow job id/state for job name `ingest-<job_id>` (used when the event or a replay has no Dataflow id) | context |

Removed: `get_workflow_execution` and the `workflow_execution` log preset (kept only while v1 events are replayed).

**Default evidence order** for a FAILED job: DAG run → task instances → failed task's log → (if a Dataflow job exists)
Dataflow job → Dataflow errors → Dataflow step logs → quotas and active jobs. REJECTED jobs skip the Dataflow error
calls, as today.

No Cloud Logging fallback for Airflow logs: those entries can only be scoped by task and time, not by DAG run, so other
runs' errors could leak into this incident's signal. When Composer is down, the Airflow tools fail (recorded as evidence)
and the investigation continues with Dataflow and the event's own `task_error`.

**Failing layer** (deterministic, from evidence; shown in the report facts and the investigation path):
| Layer | Rule | Typical cause |
|---|---|---|
| `AIRFLOW` | a task other than `run_dataflow` failed, or `run_dataflow` failed with **no** Dataflow job | listener/register/finalize bug, BQ DML error, launch `HttpError` (bad template, missing IAM `actAs`), `AirflowTaskTimeout`, zombie/killed worker |
| `DATAFLOW` | the Dataflow job ended FAILED/CANCELLED | worker exception, quota, permissions, encoding |
| `PIPELINE` | Dataflow `DONE` but status FAILED | reconciliation mismatch |
| `DATA` | status REJECTED | failed validation checks |

The report gets an **Investigation path** table, for example:
`Airflow gcs_to_bq_dataflow / file__17305…__customer_20261021.csv → task run_dataflow FAILED (try 1, 6m12s) →
Dataflow ingest-9f3c… JOB_STATE_FAILED → step ParseRecord → UnicodeDecodeError: 'utf-8' codec can't decode byte 0xe9`.

**New signatures** (`brain/signatures.yaml`) with runbooks:
| id | Pattern (signal text only) | Category |
|---|---|---|
| `airflow_task_timeout` | `AirflowTaskTimeout`, `Timeout, PID:` | TRANSIENT_INFRA (0.7, needs human approval to retry) |
| `airflow_zombie` | `Detected zombie job`, `was killed externally`, `SIGTERM`, `Task received SIGTERM` | TRANSIENT_INFRA (0.85) |
| `dataflow_launch_rejected` | `flexTemplates:launch` / `launch_flex_template` with `HttpError 400`, `INVALID_ARGUMENT` | CONFIG (0.88) |
| `airflow_dag_code_error` | traceback with a frame in `/dags/…py` | CODE_BUG (0.8) |

Existing signatures (permission, quota, encoding, schema, …) are unchanged and now also match the Airflow task log.
Retries are still the agent's create-only copy `invalid/` → `landing/`, which makes a new generation and so a new DAG
run. The agent never clears or triggers Airflow runs, and IAM + Airflow Viewer make sure it can't.

## 16. Change list (implementation plan)
| Area | Change |
|---|---|
| `infra/composer.tf` | Composer 3 env (count on `enable_composer`), `dfa-composer` SA + service-agent binding, outputs `composer_env`, `dag_gcs_prefix`, `airflow_uri` |
| `infra/pubsub.tf` | add `dfa-file-landed-airflow`, publisher binding for `dfa-composer` on errors |
| `infra/iam.tf` | `workflow` SA → `composer` SA roles; remove Pub/Sub token-creator |
| `infra/workflows.tf`, `workflows/` | delete |
| `infra/storage.tf`, `apis.tf`, `variables.tf`, `outputs.tf`, `agent.tf` | claims rule removed; Composer API; `enable_composer`, `composer_image`; outputs; agent `composer.user` |
| `dags/` | rewrite both DAGs as in section 3; `dfa_common/` gets `bq.py` (parameterised DML), `dataflow.py` (find job, error messages), `events.py` (v2 error event); `dags/tests/` |
| `Makefile`, `scripts/gcs_check.sh` | targets in section 8 |
| `df-agent/` | v2 event parsing, Airflow tools + clients, failing-layer + investigation path, signatures/runbooks, `brain/system.md`, tests, `df-agent.md` §20 |
