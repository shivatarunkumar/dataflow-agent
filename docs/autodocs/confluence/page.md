
# dataflow-agent

## Overview

dataflow-agent is a GCS → BigQuery file-ingestion pipeline on Google Cloud, orchestrated by Apache Airflow on Cloud Composer 3, plus an incident-response agent that investigates failed loads.

A delimited CSV dropped into the landing bucket is validated against a per-entity data contract. If every check passes, it is loaded into a BigQuery `raw.<entity>` table and reconciled by row count. Every run and every check is recorded in audit tables. When a file is rejected or a job fails, an error event is published. `df-agent`, a LangGraph multi-agent program you run locally, picks up that event and investigates from the Airflow DAG run down to the Dataflow error. It classifies the root cause, retries the file automatically when the failure was transient, and otherwise writes an incident report and a ServiceNow change request draft for a human.

Key capabilities:

- One Airflow DAG run per landed file generation, started by GCS notification → Pub/Sub → the `pubsub_file_listener` DAG.
- Exactly one DAG run per file generation, even when Pub/Sub delivers a message twice, because the `run_id` is derived from the generation.
- A Dataflow Flex Template (Apache Beam, Python) that runs seven ERROR-level checks: contract match, file name, header, delimiter, trailer, data types and post-load reconciliation.
- Whole-file rejection: any failed pre-load check means nothing is loaded.
- A global concurrency limit on Dataflow jobs (default 1), enforced by Airflow, so jobs fit the project's external-IP quota.
- Composer is off by default and created per session, because it bills while idle. Files that land while it is down wait on Pub/Sub for up to 7 days.
- An incident agent with deterministic policy, create-only retries, read-only Airflow access, and IAM that cannot delete anything.

**Summary for managers:** files that partners drop into a bucket are checked, loaded into BigQuery and audited automatically. Airflow costs about $0.80-1.00 per hour while its environment exists, so it is switched on only when needed; everything else is about $0 when idle. When a file is bad or a load breaks, an agent explains why within minutes. It re-runs the file on its own only for temporary platform problems; everything else becomes a report and a change request draft for an engineer.

What it is not: it is not a streaming pipeline, not a general ETL framework (it appends rows; it does not transform or deduplicate them), and the agent never changes code, configuration or data.

> [!WARNING]
> Current state at this revision: the Flex Template image has not been built successfully since the move to Airflow. Google's launcher base image fails to unpack with `archive/tar: invalid tar header`. See [Troubleshooting](#troubleshooting). Until an image is published, launches fail, so the end-to-end flow is not yet proven on Composer.

## Architecture

[Architecture diagram](diagrams/architecture.drawio)

[Interactive architecture view](diagrams/architecture.html)

| Component | Responsibility | Technology | Owning code |
|---|---|---|---|
| Landing bucket `<project>-dfa-landing` | Receives files under `landing/`. Processed files move to `archive/YYYY/MM/DD/` or `invalid/YYYY/MM/DD/`. `manual/` is for manual runs and has no notification. | GCS | `infra/storage.tf:5-15` |
| Topic `dfa-file-landed` | Receives `OBJECT_FINALIZE` notifications for the `landing/` prefix | Pub/Sub | `infra/pubsub.tf:1-25` |
| Subscription `dfa-file-landed-airflow` | Holds file events for Airflow. It is independent of Composer, with 7-day retention. | Pub/Sub pull subscription | `infra/pubsub.tf:27-43` |
| Composer environment `dfa-composer` | Runs Airflow, smallest size, only while `enable_composer = true` | Cloud Composer 3, Airflow 2.10.5 | `infra/composer.tf:9-85` |
| DAG `pubsub_file_listener` | Every 2 minutes, pulls notifications and triggers one ingestion run per file generation | Airflow | `dags/pubsub_file_listener.py:30-59` |
| DAG `gcs_to_bq_dataflow` | `register_job` → `is_new_file` → `run_dataflow` → `finalize` | Airflow | `dags/gcs_to_bq_dataflow.py:124-292` |
| Dataflow bucket `<project>-dfa-dataflow` | Holds `templates/`, `temp/` and `staging/` | GCS with a lifecycle rule on `temp/` | `infra/storage.tf:16-34` |
| Dataflow Flex job | Validates, loads, reconciles and writes audit rows | Apache Beam 2.77 on Python 3.12 | `dataflow-job/pipeline/pipeline.py:90-126` |
| Artifact Registry `dfa-dataflow` | Stores the `gcs-to-bq:latest` launcher image | Docker image | `infra/artifact_registry.tf:1-13`, `dataflow-job/Dockerfile:1-18` |
| `raw.customer`, `raw.user`, `raw.orders` | Target tables, with `_job_id`, `_source_file` and `_loaded_at` lineage columns | BigQuery, day-partitioned on `_loaded_at` | `infra/bigquery.tf:31-43` |
| `file_audit.job_metadata`, `validation_metadata`, `agent_incidents` | Run history, per-check results, agent decisions | BigQuery, day-partitioned on `created_at` | `infra/bigquery.tf:17-29`, `infra/agent.tf:10-20` |
| Topic `dfa-ingest-errors` and pull subscription `dfa-ingest-errors-sub` | One self-contained event per FAILED or REJECTED job, kept for 7 days | Pub/Sub | `infra/pubsub.tf:45-61` |
| `df-agent` | Investigates each error event and retries or reports | Python, LangGraph, Google Cloud client libraries | `df-agent/incident_core/graph.py:71-201` |
| Ollama `llama3.2` | Default LLM for every agent role. It can be switched to Anthropic per role. | Local Ollama at `localhost:11434` | `df-agent/config.yaml:9-17` |

**Boundaries and identities.**

- Everything except `df-agent` and Ollama runs in one GCP project, by default `bigquerytarun` in `europe-west2` (`Makefile:1-2`).
- Three service accounts are created (`infra/iam.tf:1-73`, `infra/agent.tf:5-86`):
  - `dfa-composer` is the identity of the Composer environment. It can launch Dataflow, run BigQuery DML, pull the file subscription, publish error events and act as `dfa-dataflow`. It always exists, so its IAM survives `make airflow-down`.
  - `dfa-dataflow` is the worker identity. It has `bigquery.dataEditor` and `jobUser`.
  - `dfa-agent` is read-only, plus `storage.objectCreator` on the landing bucket, write access to `agent_incidents` only, subscriber on the error subscription, and `composer.user` for read-only Airflow API calls.
- Your gcloud user impersonates `dfa-agent` when the agent runs locally (`infra/agent.tf:79-86`, `df-agent/domains/dataflow_ingest/clients.py:19-29`).
- New Airflow users get the Viewer role (`infra/composer.tf:37-43`), which is what keeps the agent read-only inside Airflow. `make airflow-admin` promotes your own account.

Flow diagrams, each explained under [How it works](#how-it-works):

- Per-file ingestion sequence: `diagrams/ingest-file.drawio`
- Job status lifecycle: `diagrams/job-status.drawio`
- Beam pipeline dataflow: `diagrams/beam-pipeline.drawio`
- df-agent incident graph: `diagrams/agent-incident.drawio`

## Prerequisites

- **Python 3.12.** The Makefile creates its venvs with `python3.12` (`Makefile:61-64`, `Makefile:186-190`), and the launcher image is `python312-template-launcher-base` (`dataflow-job/Dockerfile:1-4`).
- **Terraform ≥ 1.5** with the `hashicorp/google` provider `~> 6.0` (`infra/versions.tf:1-10`). State is local in `infra/`.
- **Google Cloud SDK** (`gcloud`, `bq`), authenticated with `gcloud auth login` and `gcloud auth application-default login`.
- **Docker Desktop**, running, for `make docker`. On Apple Silicon the `linux/amd64` build is emulated and slow the first time (`Makefile:87-91`).
- **A GCP project** where you are Owner or Editor. Terraform enables the 10 APIs listed in `infra/apis.tf:1-22`.
- **Quota:** each Dataflow job uses about 2 external IPs (launcher VM + 1 worker). The default `IN_USE_ADDRESSES` quota of 4 is why `max_concurrent_jobs` defaults to 1 (`infra/variables.tf:36-40`). Composer 3 uses no external IPs in the project.
- **For df-agent:** [Ollama](https://ollama.com) with the `llama3.2` model pulled, or an `ANTHROPIC_API_KEY` if you switch roles to Anthropic (`df-agent/config.yaml:9-17`).

## Installation and setup

From a clean checkout, run these in order. Everything is driven by the Makefile, and `PROJECT_ID`, `REGION` and `PREFIX` can be overridden per command, for example `make plan PROJECT_ID=my-project`.

```bash
# 1. Local Python environment and tests (free)
make venv            # .venv: apache-beam[gcp]==2.77.0, PyYAML, pytest, openpyxl
make test            # pipeline unit tests on DirectRunner, no GCP needed
make dags-test       # DAG logic and error-event tests, no Airflow server needed

# 2. Infrastructure, without Composer (free)
make plan            # terraform init + plan
make gcs-setup       # buckets, Pub/Sub, BigQuery, IAM, Artifact Registry

# 3. Flex Template image and spec
make docker          # build linux/amd64 image, push to Artifact Registry, then `make template`

# 4. Verify
make gcs-check       # every resource exists; "Composer is down" is a warning

# 5. Start Airflow (about 25 minutes; billing starts here)
make airflow-up      # creates Composer and uploads dags/
make airflow-ui      # prints the UI URL; open it and sign in once
make airflow-admin   # gives your account Admin in Airflow
```

`make gcs-check` reports the Flex Template image and spec only as warnings (`scripts/gcs_check.sh:1-133`). Everything else must be OK.

> [!WARNING]
> End every session with `make airflow-down`. Composer costs about $0.80-1.00 per hour while it exists, even with no files to process.

Then set up the agent:

```bash
make agent-venv        # .venv-agent: LangGraph, LangChain, Google clients, reportlab
make agent-test        # policy matrix, signatures, brain coverage, graph scenarios (no GCP, no LLM)
make agent-brain-sync  # copy BQ schemas into df-agent/brain/schemas and check runbook coverage
ollama serve           # in another terminal, if Ollama is not already running
```

`make agent-brain-sync` prints `brain OK` when every signature runbook and every category playbook exists (`df-agent/incident_core/runner.py:180-192`).

## Configuration

Configuration lives in four places. The Makefile turns Terraform outputs into environment variables, so you normally change only Makefile variables or Terraform variables.

**Makefile variables** (pass them on the command line):

| Variable / key | Required | Default | Purpose | Read at |
|---|---|---|---|---|
| `PROJECT_ID` | no | `bigquerytarun` | Target GCP project | `Makefile:1` |
| `REGION` | no | `europe-west2` | Region for every regional resource | `Makefile:2` |
| `PREFIX` | no | `dfa` | Resource name prefix | `Makefile:3` |
| `AGENT_OPERATOR` | no | your `gcloud` account | IAM member allowed to impersonate `dfa-agent` | `Makefile:6` |
| `MAX_JOBS` | no | `1` | Passed as `max_concurrent_jobs`. `4` deliberately causes `QUOTA_EXCEEDED` for agent tests. | `Makefile:10` |
| `COMPOSER` | no | current state | `true` or `false` overrides whether `make gcs-setup` keeps Composer | `Makefile:17` |
| `BASE_IMAGE` | no | pinned `python312` release of 2026-09-01 | Flex Template launcher base image | `Makefile:28` |
| `FILE` | for `run-dataflow`, `run-local` | — | Mock file name or `gs://` URI | `Makefile:52` |
| `JOB_ID`, `INCIDENT`, `EXPECTED`, `NOTE` | for agent targets | — | Job or incident to replay, approve or save as an eval case | `Makefile:209-225` |
| `CONFIRM` | no | — | `yes` skips the `bq-clean-up` prompt | `Makefile:129-144` |

**Terraform variables** (`infra/variables.tf:1-58`, example in `infra/terraform.tfvars.example:1-3`):

| Variable / key | Required | Default | Purpose | Read at |
|---|---|---|---|---|
| `project_id` | yes | — | Project to deploy into | `infra/variables.tf:1-4` |
| `region` | no | `europe-west2` | Region | `infra/variables.tf:6-10` |
| `prefix` | no | `dfa` | Buckets are named `<project_id>-<prefix>-*` | `infra/variables.tf:12-16` |
| `audit_dataset` | no | `file_audit` | Audit dataset | `infra/variables.tf:18-22` |
| `target_dataset` | no | `raw` | Target dataset | `infra/variables.tf:24-28` |
| `agent_operator` | no | `""` (nobody) | Member granted Token Creator on `dfa-agent` | `infra/variables.tf:30-34` |
| `max_concurrent_jobs` | no | `1` | Dataflow jobs allowed at once | `infra/variables.tf:36-40` |
| `enable_composer` | no | `false` | Whether the Composer environment exists | `infra/variables.tf:42-46` |
| `composer_image` | no | `composer-3-airflow-2.10.5` | Composer image version | `infra/variables.tf:48-52` |
| `target_tables` | no | `customer`, `user`, `orders` | Each needs `infra/bq/<name>.json` | `infra/variables.tf:54-58` |

**Airflow environment.** Terraform sets `DFA_PROJECT_ID`, `DFA_REGION`, `DFA_LANDING_BUCKET`, `DFA_DATAFLOW_BUCKET`, `DFA_SUBSCRIPTION`, `DFA_ERROR_TOPIC`, `DFA_TEMPLATE_PATH`, `DFA_AUDIT_DATASET`, `DFA_TARGET_DATASET`, `DFA_DATAFLOW_SA`, `DFA_MAX_CONCURRENT_JOBS` and `DFA_COMPOSER_ENV` on the environment (`infra/composer.tf:22-35`). The DAGs read them in `dags/dfa_common/config.py:1-34`. Change them through Terraform, not in the console; each change updates the environment and takes several minutes.

**Pipeline options** (Flex Template parameters, `dataflow-job/pipeline/options.py:8-19`):

| Variable / key | Required | Default | Purpose | Read at |
|---|---|---|---|---|
| `--input_file` | yes | — | `gs://` URI, or a local path for DirectRunner | `dataflow-job/main.py:18-19` |
| `--audit_job_id` | unless `trigger_type=MANUAL` | generated UUID for MANUAL | `job_metadata.job_id` registered by the workflow | `dataflow-job/main.py:23-26` |
| `--trigger_type` | no | `MANUAL` | `PUBSUB` or `MANUAL` | `dataflow-job/pipeline/options.py:16` |
| `--file_generation`, `--pubsub_message_id` | no | — | Recorded on manual inserts | `dataflow-job/pipeline/options.py:14-15` |
| `--audit_dataset` / `--target_dataset` | no | `file_audit` / `raw` | Datasets | `dataflow-job/pipeline/options.py:17-18` |
| `--contracts_dir` | no | bundled `contracts/` | Contract YAML directory | `dataflow-job/pipeline/contracts.py:73-78` |


**Data contracts.** `dataflow-job/contracts/<entity>.yaml` sets the file prefix, the name pattern, the delimiter, the target table and typed columns (`dataflow-job/contracts/customer.yaml:1-16`). The longest matching `<file_prefix>_` wins (`dataflow-job/pipeline/contracts.py:81-84`). Contracts are baked into the image, so rebuild with `make docker` after changing one.

**df-agent** (`df-agent/config.yaml:1-60`). `${VAR:-default}` placeholders are expanded from the environment, and relative directories resolve against `df-agent/` (`df-agent/incident_core/config.py:11-27`). The Makefile exports `DFA_PROJECT_ID`, `DFA_REGION`, `DFA_LANDING_BUCKET`, `DFA_DATAFLOW_BUCKET`, `DFA_ERROR_SUBSCRIPTION`, `DFA_AUDIT_DATASET`, `DFA_COMPOSER_ENV`, `DFA_AGENT_SA` and `DFA_DATAFLOW_SA` (`Makefile:46-49`). Other settings:

- `OLLAMA_BASE_URL` (default `http://localhost:11434`)
- `llm.default` and `llm.roles` (provider `ollama`, `anthropic` or `none` for rules-only mode)
- `features.generate_patches` (default `false`)
- `limits.evidence_rounds` (default 2)
- `change.*`: service name, assignment group, requester, data owner and ops channel used in change request drafts (`df-agent/config.yaml:41-48`). `DFA_CHANGE_GROUP` and `DFA_CHANGE_REQUESTER` override two of them.
- `impersonate_service_account` (empty means use your own ADC)

**Secrets.** The repository holds no secrets. GCP access is ADC plus service-account impersonation. `ANTHROPIC_API_KEY` is needed only if a role uses `provider: anthropic`; keep it in your shell, not in `config.yaml`.

## Usage

**Process files end to end.** With Composer up, drop files into `landing/` and watch them run:

```bash
gcloud storage cp mockdata/customer_20261015.csv gs://bigquerytarun-dfa-landing/landing/
make upload-mock     # or: every mock file (about 75 minutes, one Dataflow job at a time)
make airflow-runs    # recent gcs_to_bq_dataflow DAG runs and their state
make errors          # peek at up to 10 error events (not acked)
```

To ingest a real file, upload `<entity>_<YYYYMMDD>.csv` to `gs://<project>-dfa-landing/landing/`. Its last line must be the trailer `T|<record_count>|<YYYYMMDD>`. The listener picks it up within 2 minutes.

**Check the results in BigQuery:**

```sql
SELECT file_name, status, rows_in_file, rows_loaded, recon_status, final_file_path
FROM file_audit.job_metadata ORDER BY created_at DESC;

SELECT file_name, check_name, validation_result, summary
FROM file_audit.validation_metadata ORDER BY created_at DESC;
```

Expected outcomes for the mock files:

- `customer_20261015.csv`, `user_20261015.csv`, `orders_20261015.csv` and `customer_20261017.csv` end `SUCCESS`.
- `user_20261016.csv` ends `REJECTED` on `header`.
- `customer_20261018.csv` ends `REJECTED` on `trailer`.
- `orders_20261016.xlsx` ends `REJECTED` on `file_name`.
- `customer_20261021.csv` is Latin-1 encoded and ends `FAILED` with `UnicodeDecodeError`.

The full list is in `project.md` §10. Regenerate the mock files with `make mockdata`.

**Test a failure before Dataflow.** `make airflow-test-launch-failure FILE=customer_20261015.csv` runs one file through Airflow with a missing template path, so it fails before any Dataflow job exists (`Makefile:175-179`).

**Run the pipeline without Airflow:**

```bash
make run-dataflow FILE=customer_20261015.csv            # copies to manual/ (no trigger) and runs on Dataflow
make run-local FILE=mockdata/customer_20261015.csv      # DirectRunner, writes to real BigQuery
```

Manual runs use `trigger_type=MANUAL`. The pipeline's MERGE inserts the `job_metadata` row, and the file is not moved. `run-dataflow` is outside Airflow's concurrency limit, so do not run it during a bulk upload (`Makefile:100-124`).

**Reset the tables between tests:**

```bash
make bq-clean-up              # TRUNCATE job_metadata, validation_metadata and every raw.* table; asks for "yes"
make bq-clean-up CONFIRM=yes  # no prompt
```

**Run the incident agent:**

```bash
make agent-listen                        # handle error events one at a time (Ctrl-C to stop)
make agent-once                          # at most one pending event, then exit
make agent-replay JOB_ID=<job_id>        # re-investigate a FAILED or REJECTED job without Pub/Sub
make agent-approve INCIDENT=<job_id>     # approve pending NEEDS_HUMAN actions (e.g. a retry)
make agent-graph                         # draw the LangGraph workflow to df-agent/graph.mmd
make agent-eval                          # replay recorded cases in df-agent/evals/cases
make agent-eval-add INCIDENT=<id> EXPECTED=CONFIG NOTE="..."
```

Each run prints an investigation trace and ends with a line like `RESULT <id> | status AWAITING_HUMAN | root cause DATA_QUALITY (0.95) | executed nothing | ...` (`df-agent/incident_core/runner.py:45-73`). Each incident's output lands in `df-agent/actions/<incident_id>/`, which is git-ignored (`df-agent/incident_core/report/writer.py:43-103`):

- `report.pdf`: the incident report.
- `change.md`: a ServiceNow change request draft, or the reason none is needed.
- `details/`: `report.md`, `change_request.json`, `evidence.json`, `state.json`, `review.md`, `trace.jsonl`, `trace.md`, `llm/`, and `history/` for earlier runs.

## How it works

### Per-file ingestion (Airflow)

[Per-file ingestion sequence](diagrams/ingest-file.drawio)

[Interactive per-file ingestion view](diagrams/ingest-file.html)

The `pubsub_file_listener` DAG runs every 2 minutes, one run at a time. `gcs_to_bq_dataflow` does the work for each file:

1. **Pull.** `PubSubPullSensor` pulls up to 10 messages and acks them. If none arrive within 100 s, the run is skipped, not failed (`dags/pubsub_file_listener.py:42-52`).
2. **Filter and trigger.** `file_event` ignores anything other than `OBJECT_FINALIZE`, objects outside `landing/`, and folder markers. It builds `run_id = file__<generation>__<name>` (`dags/dfa_common/logic.py:15-40`). A mapped `TriggerDagRunOperator` starts one run per file. An existing `run_id`, which means a duplicate delivery, is skipped (`dags/pubsub_file_listener.py:54-59`).
3. **Register.** `register_job` generates a UUID `job_id` and counts earlier attempts for the same `file_path` and generation. It inserts `IN_PROGRESS`, or `DUPLICATE_SKIPPED` if that generation already succeeded, with `service_name = 'composer'` and `service_revision` set to the DAG run id (`dags/gcs_to_bq_dataflow.py:138-200`).
4. **Stop duplicates.** `is_new_file` short-circuits the rest of the run for a duplicate (`dags/gcs_to_bq_dataflow.py:202-204`).
5. **Launch and wait.** `run_dataflow` launches the Flex Template as job `ingest-<job_id>` with `maxWorkers: 1`, and waits for it. `max_active_tis_per_dag` caps concurrent jobs, so other files wait as scheduled task instances. After 60 minutes the task times out (`dags/gcs_to_bq_dataflow.py:208-240`).
6. **Finalize.** `finalize` always runs. It finds the Dataflow job by name, so it also works when `run_dataflow` failed, and reads the status Dataflow wrote. It forces `FAILED` if the job did not end `JOB_STATE_DONE` or the row is still `IN_PROGRESS`, with `DATAFLOW_JOB_FAILED`, or `AIRFLOW_TASK_FAILED` when no job was ever created (`dags/gcs_to_bq_dataflow.py:243-264`, `dags/dfa_common/logic.py:43-53`). It then moves the file to `archive/` (SUCCESS) or `invalid/` (anything else) and records `final_file_path` (`dags/gcs_to_bq_dataflow.py:266-272`, `dags/gcs_to_bq_dataflow.py:295-306`).
7. **Publish.** For FAILED or REJECTED, `finalize` publishes a schema v2 error event. It holds the job row, failed checks, Dataflow errors, the Airflow task states and the failed task's log tail (`dags/gcs_to_bq_dataflow.py:274-286`, `dags/dfa_common/events.py:36-81`). A publish failure is logged and does not change the outcome. A FAILED status then fails the task, so the DAG run is red; a REJECTED run stays green (`dags/gcs_to_bq_dataflow.py:288-290`).
8. **Safety net.** If a run fails without `finalize` publishing, for example because `register_job` crashed, the DAG's `on_failure_callback` marks the job FAILED and publishes a minimal event (`dags/gcs_to_bq_dataflow.py:95-121`).

### Job status lifecycle

[Job status lifecycle](diagrams/job-status.drawio)

[Interactive job status view](diagrams/job-status.html)

`file_audit.job_metadata.status` moves from `IN_PROGRESS` to one of four terminal values:

- `register_job` writes `IN_PROGRESS` or `DUPLICATE_SKIPPED` (`dags/gcs_to_bq_dataflow.py:156-158`).
- The Dataflow `FinalizeJobMetadata` MERGE writes the outcome. It is `REJECTED` if any pre-load check is blocking (error code = the first failed check's name), `FAILED` / `RECON_MISMATCH` if reconciliation failed or did not run, and `SUCCESS` otherwise (`dataflow-job/pipeline/transforms/audit.py:74-110`).
- `finalize` can still override the result to `FAILED` afterwards (`dags/dfa_common/logic.py:43-53`).

`job_metadata` is written only with DML (INSERT, UPDATE or MERGE), never with streaming inserts, so rows can always be updated (`dataflow-job/pipeline/transforms/audit.py:113-117`). For a manual run the MERGE inserts the row itself, with `service_name = 'dataflow'` (`dataflow-job/pipeline/transforms/audit.py:171-179`).

A FAILED or REJECTED file is not final. Re-uploading a fixed file, or a df-agent `RETRY_FILE` copy from `invalid/` back to `landing/`, creates a new object generation and therefore a new DAG run and job.

### Beam pipeline: validate, gate, load, reconcile

[Beam pipeline dataflow](diagrams/beam-pipeline.drawio)

[Interactive Beam pipeline view](diagrams/beam-pipeline.html)

`main.py` parses options, loads the contracts and builds the graph (`dataflow-job/main.py:14-44`). The graph uses only Beam transforms. Side inputs carry shared state (`dataflow-job/pipeline/pipeline.py:24-126`).

- **Structure checks.** `ValidateFileStructure` reads only the first line and the last 64 KiB of the file. It emits `contract_match`, `file_name` and `header` results, plus a `FileMeta` side input that carries the matched contract, the header, the trailer and the trailer count (`dataflow-job/pipeline/transforms/file_checks.py:14-61`).
- **Parse.** `ReadFromText(skip_header_lines=1)` → `IsDataLine` drops blank lines and the trailer → `ParseRecord` splits each line on the contract delimiter. A wrong field count is tagged `delimiter_error`, a NOT_NULL or type failure is tagged `type_error`, and valid rows get the lineage columns (`dataflow-job/pipeline/transforms/parse.py:6-38`).
- **Content checks.** `BuildContentChecks` runs once, after the row counts and error samples are aggregated, and emits `delimiter`, `trailer` and `data_type` results (`dataflow-job/pipeline/transforms/file_checks.py:76-85`). The rules themselves are in `dataflow-job/pipeline/checks.py:66-243`.
- **Gate and load.** `AllChecksPassed` combines every pre-load result into `file_ok` (`dataflow-job/pipeline/transforms/combiners.py:37-50`). `LoadToTargetTable` filters on it and writes with `FILE_LOADS`, `CREATE_NEVER` and `WRITE_APPEND`. The destination table and schema come from the contract side input (`dataflow-job/pipeline/transforms/load.py:5-33`).
- **Reconcile.** `ReconcileCounts` waits on every load and copy job, then compares the file's data rows, the trailer count and `COUNT(*) WHERE _job_id = @job_id`. It is SKIPPED for rejected files (`dataflow-job/pipeline/transforms/recon.py:6-47`, `dataflow-job/pipeline/checks.py:245-270`).
- **Audit.** Every result is streamed into `validation_metadata`, with JSON columns sent as strings (`dataflow-job/pipeline/transforms/audit.py:13-71`). `FinalizeJobMetadata` then MERGEs the final status and counts (`dataflow-job/pipeline/transforms/audit.py:113-208`).

### Incident agent (df-agent)

[df-agent incident graph](diagrams/agent-incident.drawio)

[Interactive df-agent view](diagrams/agent-incident.html)

**Listener.** `runner.py listen` pulls one message at a time and extends its ack deadline to 600 s while the graph runs. It acks after the run, and on an exception resets the deadline to 120 s so the message is redelivered. A message it cannot parse is saved to `actions/_unparseable/` and acked (`df-agent/incident_core/runner.py:81-122`). `replay` rebuilds the event from the audit tables (`df-agent/domains/dataflow_ingest/pack.py:86-97`). Schema v2 is Airflow; schema v1 (Cloud Workflows) events still parse so old incidents can be replayed (`df-agent/domains/dataflow_ingest/event.py:14-64`).

**Graph** (`df-agent/incident_core/graph.py:163-201`):

- **intake** skips incidents already recorded as handled. It closes `SUCCESS`, `IN_PROGRESS` and `DUPLICATE_SKIPPED` jobs as `NO_INCIDENT` and sets the severity: SEV2 for FAILED, SEV3 for REJECTED, one level higher after 3 or more failures in 24 h (`df-agent/incident_core/graph.py:76-96`, `df-agent/domains/dataflow_ingest/pack.py:99-124`).
- **audit_agent** and **gcp_agent** run in parallel. Each runs a fixed set of default lookups, and an LLM may choose up to 4 extra read-only tools only when the investigator asks (`df-agent/incident_core/agents/gatherer.py:65-148`). Tools are typed functions with fixed queries, not a shell (`df-agent/domains/dataflow_ingest/tools_gcp.py:67-268`).
- **Top-down evidence.** For an Airflow job, `gcp_agent` reads the DAG run, then the task instances, then the failed task's log. If the event has no Dataflow job id, it confirms with `find_dataflow_job`. Only then does it read the Dataflow job, errors and logs, the GCS object, quotas and active jobs (`df-agent/domains/dataflow_ingest/tools_gcp.py:279-299`). The Airflow client implements only GET (`df-agent/domains/dataflow_ingest/clients.py:90-124`).
- **log_analyst** builds a deterministic error digest, matches `brain/signatures.yaml`, and runs verification calls for the top signature. A CONFIRMED verification raises its confidence to at least 0.97; a REFUTED one caps it at 0.5 (`df-agent/incident_core/agents/log_analyst.py:24-103`).
- **investigator** sets the root cause to one of TRANSIENT_INFRA, DATA_QUALITY, CODE_BUG, CONFIG or UNKNOWN. A signature at or above `signature_override_confidence` (0.85) decides the category, and the LLM only writes the explanation. Without the LLM it falls back to the signature, or to UNKNOWN. It can request more evidence up to `limits.evidence_rounds` times (`df-agent/incident_core/agents/investigator.py:50-153`).
- **action_planner** maps the category to its playbook actions (`df-agent/brain/playbooks.yaml:1-40`) and fills in parameters. For `RETRY_FILE` these are a copy from `invalid/...` back to the original `landing/...` name (`df-agent/domains/dataflow_ingest/pack.py:138-151`). A `PROPOSE_CODE_CHANGE` action routes through **code_reviewer** (`git apply --check` only).
- **action_reviewer** runs `PolicyEngine.evaluate` first, which is authoritative. Unknown types, forbidden patterns (`delete`, `drop`, `truncate`, `rm`, `mv`, `--force`, …) and bad parameters are REJECTED, and non-executable types become REPORT. `RETRY_FILE` is AUTO_APPROVED only when all of these hold: the job FAILED, the category is TRANSIENT_INFRA, confidence ≥ 0.7, fewer than 2 retries in 24 h, the source exists, the destination does not exist, and no stuck ingest jobs remain. Otherwise it is NEEDS_HUMAN if fewer than 5 retries were made (`df-agent/brain/policies.yaml:12-57`, `df-agent/incident_core/policy.py:64-92`). The LLM review may only downgrade AUTO_APPROVED to NEEDS_HUMAN (`df-agent/incident_core/agents/reviewers.py:81-110`).
- **executor** runs only AUTO_APPROVED actions. `RETRY_FILE` is a GCS rewrite with `if_generation_match=0` on the landing bucket and has no delete path (`df-agent/domains/dataflow_ingest/executor.py:8-30`). The final status is AUTO_RESOLVED or AWAITING_HUMAN (`df-agent/incident_core/graph.py:109-135`).
- **report_writer** and **record** write `actions/<id>/`. They append a row to `file_audit.agent_incidents`, mirrored to `.state/incidents.jsonl`. If the BigQuery write fails, the local mirror still records the incident (`df-agent/incident_core/store.py:84-112`).

**Failing layer.** The report names where the failure happened and shows an investigation path from the DAG run to the error (`df-agent/domains/dataflow_ingest/pack.py:27-46`, `df-agent/domains/dataflow_ingest/pack.py:269-305`):

| Layer | Rule |
|---|---|
| `AIRFLOW` | A task other than `run_dataflow` or `finalize` failed, or `run_dataflow` failed and no Dataflow job exists |
| `DATAFLOW` | The Dataflow job ended FAILED or CANCELLED |
| `PIPELINE` | The Dataflow job ended DONE but the status is FAILED (reconciliation) |
| `DATA` | The status is REJECTED |

**Change request draft.** `change.md` is drafted when a human must change the service. Code and config fixes are Normal changes (Emergency at SEV1). A retry waiting for approval is a Standard change. An automatic retry, a rejected file or an unclear root cause needs no change request, and `change.md` says why (`df-agent/domains/dataflow_ingest/change.py:48-83`). Type, risk, impact, urgency and priority are computed, not written by the LLM (`df-agent/domains/dataflow_ingest/change.py:119-137`). The same content is saved as a ServiceNow Table API payload in `details/change_request.json` (`df-agent/incident_core/report/change.py:31-42`). The agent never creates records in ServiceNow.

**Human approval.** `make agent-approve` re-checks each NEEDS_HUMAN action against fresh policy facts, asks `Execute? [y/N]`, then records `APPROVED_BY_HUMAN` (`df-agent/incident_core/runner.py:128-177`).

### Error handling, retries and concurrency

- **Pub/Sub at-least-once delivery:** absorbed by the deterministic `run_id` and the `DUPLICATE_SKIPPED` check (`dags/dfa_common/logic.py:15-17`, `dags/gcs_to_bq_dataflow.py:156-158`).
- **Lost events:** messages are acked when pulled, so a failed trigger task is covered only by that task's 2 Airflow retries (`dags/pubsub_file_listener.py:38-47`).
- **Airflow retries:** the ingestion DAG has none (`dags/gcs_to_bq_dataflow.py:124-135`). A failed file is retried by re-uploading it, or by df-agent.
- **Moving the file:** if the object is gone or was replaced, the move is logged and skipped (`dags/gcs_to_bq_dataflow.py:295-306`). A failure to publish the error event is logged and does not change the outcome (`dags/gcs_to_bq_dataflow.py:274-286`).
- **Concurrency:** limited to `DFA_MAX_CONCURRENT_JOBS` Dataflow jobs. Files wait as scheduled task instances, so `duration_seconds` includes the time spent waiting.
- **Agent:** handles one incident at a time, and uses a SQLite checkpointer per run (`df-agent/incident_core/graph.py:204-209`).

## Deployment and operations

- **Infrastructure:** `make gcs-setup` (`terraform apply -auto-approve`, `Makefile:78-79`). Composer keeps its current state. There is no CI pipeline in the repository.
- **Airflow lifecycle:** `make airflow-up` creates Composer in about 25 minutes and uploads the DAGs. `make airflow-down` deletes only the Composer environment in about 10 minutes (`Makefile:152-158`). Airflow run history and task logs are deleted with the environment; the audit tables keep the outcome.
- **DAG changes:** `make dags-deploy` copies `dags/` to the environment's DAG folder, excluding tests. Composer picks changes up within about a minute (`Makefile:160-163`).
- **Pipeline code or contracts:** `make docker` rebuilds and pushes `gcs-to-bq:latest`, then `make template` rewrites `gs://<dataflow bucket>/templates/gcs_to_bq.json` (`Makefile:87-98`). Run `make template` alone after changing only `metadata.json`. The image tag is always `latest`, so rolling back means rebuilding from an earlier commit.
- **Environments:** one project per `PROJECT_ID`. Terraform state is local (`infra/*.tfstate`, git-ignored), so run Terraform from one machine, or move the state to a remote backend before sharing.
- **Health check:** `make gcs-check` is read-only and exits 1 on any failure (`Makefile:81-82`).
- **Logs and history:**
  - Airflow DAG runs and task logs are in the Airflow UI; `make airflow-runs` lists recent runs, and each `job_metadata` row stores the DAG run id in `service_revision`.
  - Dataflow job logs and errors are in the Dataflow console. The error event carries the console URL.
  - Agent traces are in `df-agent/actions/<id>/details/trace.md`.
- **Alerts:** no Cloud Monitoring alerts are defined. The error topic and `df-agent` are the alerting path.
- **Scaling:** raise `max_concurrent_jobs` (for example `make gcs-setup MAX_JOBS=2`) only after raising the regional `IN_USE_ADDRESSES` quota. Each job needs about 2 IPs.
- **Backup and restore:** none is configured. Buckets use `force_destroy` and datasets use `delete_contents_on_destroy`, so `make gcs-destroy` deletes every file and row (`infra/storage.tf:5-15`, `infra/bigquery.tf:1-15`).
- **Cost:** Composer costs about $0.80-1.00 per hour while it exists. A 3-hour session is about $3-5 including create and delete time. Dataflow is about $0.02 per file. Everything else is about $0 when idle (`project.md` §12).

> [!WARNING]
> `make gcs-destroy` and `make bq-clean-up` delete data irreversibly. `gcs-destroy` force-deletes both buckets and both datasets. `bq-clean-up` truncates every audit and target table.

## Troubleshooting

| Symptom | Likely cause | How to confirm | Fix |
|---|---|---|---|
| `make docker` fails with `failed to register layer: ... archive/tar: invalid tar header` | Docker cannot unpack a layer of Google's `python312-template-launcher-base`. It failed for the 2026-10-06, 10-26 and 11-02 releases, on a Mac and in Cloud Build. | The error is on step `[1/6] FROM ...`, before any of our code | **Open issue.** The default is pinned to the 2026-09-01 release, which has not been confirmed to build (`Makefile:26-28`). Try others with `make docker BASE_IMAGE=...`. |
| Launch fails with `404 ... Unable to open template file` | The template spec is missing, or the image was never built | `make gcs-check` warns about the image or the spec | `make docker`, then retry with `make agent-approve INCIDENT=<id>` |
| df-agent reports CODE_BUG for that 404 | **Known issue.** `Unable to open template file` matches no signature, so without the LLM the generic traceback signature wins (`df-agent/brain/signatures.yaml:49-60`, `df-agent/brain/signatures.yaml:169-175`) | The report shows `python_traceback` at 0.55 | Start Ollama and replay. With the LLM it was classified CONFIG. |
| The event's `orchestration.task_error` is empty | `finalize` reads the task log from the environment bucket, and it was not found at that path on Composer 3 (`dags/dfa_common/events.py:84-96`) | `make errors` shows an empty field | No action needed for diagnosis: the agent reads the same log through the Airflow API |
| `make gcs-setup` fails on the `composer.ServiceAgentV2Ext` binding in a new project | The Composer service agent does not exist yet | The error names `cloudcomposer-accounts` | Wait a minute and rerun `make gcs-setup` |
| `No terraform outputs found - run 'make gcs-setup' first.` | No local Terraform state | `terraform -chdir=infra output` is empty | Run `make gcs-setup` from this checkout (`Makefile:129-130`) |
| `make docker` says it cannot connect to the Docker daemon | Docker Desktop is not running | `docker info` | Start Docker Desktop |
| Files land but nothing happens | Composer is down | `make gcs-check` warns "Composer is down" | `make airflow-up`. Queued events are processed afterwards, up to 7 days old. |
| A file stays `IN_PROGRESS` for a long time | It is waiting for a Dataflow slot (one job at a time, about 6 min each) | In Airflow, `run_dataflow` is `scheduled` for that run | Wait, or raise the limit after raising the IP quota |
| `make airflow-admin` fails | You have not signed in to the Airflow UI yet, so no user exists | `make airflow-ui`, then sign in | Sign in once and rerun |
| The agent's Airflow tools fail | Composer is down, or still being created (`df-agent/domains/dataflow_ingest/clients.py:104-118`) | The evidence item shows the error | `make airflow-up`. The investigation continues with Dataflow and audit evidence. |
| Status `DUPLICATE_SKIPPED` | That file generation already loaded successfully | An earlier `SUCCESS` row has the same `file_path` and `file_generation` | Upload the file again to create a new generation, if you really want to reload it |
| Status `REJECTED`, error code `FILE_NAME`, `HEADER`, `TRAILER`, `DELIMITER`, `DATA_TYPE` or `CONTRACT_MATCH` | The file breaks its contract | `SELECT * FROM file_audit.validation_metadata WHERE job_id = '<id>'` shows the expected and actual values and up to 10 sample rows | Fix the file and re-upload it to `landing/`. See `df-agent/brain/runbooks/<check>.md`. |
| Status `FAILED` with `UnicodeDecodeError` | The file is not UTF-8 (e.g. Latin-1) | The Dataflow errors in the error event. df-agent matches the `encoding_error` signature as CODE_BUG (`df-agent/brain/signatures.yaml:135-141`). | Re-encode the file as UTF-8, or change the pipeline to decode it, then `make docker` |
| Status `FAILED` with `QUOTA_EXCEEDED` / `RESOURCE_EXHAUSTED` | More concurrent jobs than the IP quota allows (e.g. `MAX_JOBS=4`) | Dataflow job messages; the agent's `get_quota` evidence (`df-agent/brain/signatures.yaml:78-83`) | df-agent retries automatically (TRANSIENT_INFRA). Lower `MAX_JOBS` or raise the quota. |
| Status `FAILED`, error code `AIRFLOW_TASK_FAILED` | The launch was rejected, or a DAG task crashed, so no Dataflow job ran | The report's failing layer is `AIRFLOW`; read the failed task's log | See `df-agent/brain/runbooks/AIRFLOW_TASK_FAILED.md` and `DATAFLOW_LAUNCH_FAILED.md` |
| Status `FAILED`, error code `RECON_MISMATCH` | Rows in BigQuery ≠ file rows ≠ trailer count | `recon_row_count` details in `validation_metadata` | See `df-agent/brain/runbooks/RECON_MISMATCH.md` |
| df-agent crashes with `'change' is undefined` | The agent process started before an upgrade: Python code is loaded once, but report templates are read from disk each time | The log shows the old `actions/<id>/trace.jsonl` path | Restart `make agent-listen` |
| df-agent logs `LLM gave no valid ... falling back` or `Connection refused` | Ollama is not running, or the small model returned invalid JSON | `curl http://localhost:11434` | Start `ollama serve`. Classification still works from signatures (`df-agent/incident_core/agents/investigator.py:122-131`). |
| df-agent logs `could not record incident in BigQuery (kept locally)` | No permission on `agent_incidents`, or impersonation failed | `df-agent/.state/incidents.jsonl` has the row | Check `AGENT_OPERATOR` and rerun `make gcs-setup` (`df-agent/incident_core/store.py:91-98`) |
| `make agent-approve` prints `BLOCKED by policy now` | Facts changed: the destination already exists, the source is gone, or the retry limit is reached | The printed blockers | Resolve the blocker manually; the agent will not override policy (`df-agent/incident_core/runner.py:142-148`) |
| Error events pile up in `dfa-ingest-errors-sub` | The agent is not running | `make errors` | `make agent-listen`. Messages are kept for 7 days. |

## Source map

| Path | Responsibility |
|---|---|
| `Makefile` | Every operator command: infrastructure, Airflow, image, runs, clean-up, agent |
| `COMMANDS.md` | Deploy, test and tear-down commands in order |
| `infra/` | Terraform: APIs, buckets, Pub/Sub, Composer, BigQuery, IAM, agent identity |
| `infra/bq/*.json` | BigQuery table schemas, including `agent_incidents` |
| `dags/pubsub_file_listener.py` | Listener DAG: one ingestion run per file generation |
| `dags/gcs_to_bq_dataflow.py` | Ingestion DAG: register, launch, finalize, failure callback |
| `dags/dfa_common/` | Config, BigQuery and Dataflow helpers, error event, pure decision logic |
| `dags/tests/` | DAG logic, error event and DAG import tests (`make dags-test`) |
| `dataflow-job/main.py`, `pipeline/pipeline.py` | Pipeline entry point and graph |
| `dataflow-job/pipeline/checks.py` | Validation rules and type conversion |
| `dataflow-job/pipeline/transforms/` | File checks, parse, load, reconcile, audit writers |
| `dataflow-job/contracts/*.yaml` | Per-entity data contracts |
| `dataflow-job/tests/` | Beam `TestPipeline` unit tests (`make test`) |
| `df-agent/incident_core/` | Domain-agnostic agent core: graph, agents, policy, signatures, store, report, CLI |
| `df-agent/domains/dataflow_ingest/` | Domain pack: event parsing, audit, Airflow and GCP tools, `RETRY_FILE` executor, change request draft |
| `df-agent/brain/` | Signatures, playbooks, policies, runbooks, system prompt |
| `df-agent/tests/` | Policy, signature, error, Airflow, change request and graph scenario tests (`make agent-test`) |
| `scripts/gcs_check.sh` | Read-only resource health check |
| `mockdata/` | Sample good and bad files, and `generate.py` |

Where to start common changes:

- **Add an entity:**
  1. Add `dataflow-job/contracts/<entity>.yaml` and `infra/bq/<entity>.json`.
  2. Append the entity to `target_tables` (`infra/variables.tf:54-58`).
  3. Run `make gcs-setup` and `make docker`.
- **Add a validation rule:** write it in `dataflow-job/pipeline/checks.py`, emit it from `BuildContentChecks` or `ValidateFileStructure`, and add a test in `dataflow-job/tests/test_checks.py`.
- **Change orchestration:** edit the DAGs in `dags/`, run `make dags-test`, then `make dags-deploy`.
- **Teach the agent a new failure:** add a signature to `df-agent/brain/signatures.yaml` and a runbook under `df-agent/brain/runbooks/`, then run `make agent-brain-sync` and `make agent-test`.
- **Allow a new automatic action:** add it to `action_types`, `param_rules` and `auto_approve` in `df-agent/brain/policies.yaml`, and implement it in `DataflowIngestPack.execute` (`df-agent/domains/dataflow_ingest/pack.py:166-169`).

## Testing

```bash
make test         # dataflow-job/tests: ValidateAndParse and the checks, DirectRunner, no GCP
make dags-test    # dags/tests: DAG logic and error event; the DAG import test runs only if Airflow is installed
make agent-test   # df-agent/tests: policy matrix, signatures, brain coverage, graph scenarios with fakes
make agent-eval   # offline replay of recorded incidents; exits non-zero below 100% accuracy
```

The agent tests use fake clients and no LLM (`df-agent/tests/fakes.py:1-195`). `agent-eval` reports root-cause accuracy over `df-agent/evals/cases/` (`df-agent/incident_core/runner.py:253-259`).
