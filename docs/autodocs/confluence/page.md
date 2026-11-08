
# dataflow-agent

## Overview

dataflow-agent is a serverless GCS → BigQuery file-ingestion pipeline on Google Cloud, plus an incident-response agent that investigates failed loads.

A delimited CSV dropped into the landing bucket is validated against a per-entity data contract. If every check passes, it is loaded into a BigQuery `raw.<entity>` table and reconciled by row count. Every run and every check is recorded in audit tables. When a file is rejected or a job fails, an error event is published. `df-agent`, a LangGraph multi-agent program you run locally, picks up that event. It gathers evidence, classifies the root cause, retries the file automatically when the failure was transient, and writes an incident report for a human otherwise.

Key capabilities:

- One Cloud Workflows execution per landed file, started by GCS notification → Pub/Sub → Eventarc.
- Exactly one audit job per file generation, even when Pub/Sub delivers a message twice.
- A Dataflow Flex Template (Apache Beam, Python) that runs seven ERROR-level checks: contract match, file name, header, delimiter, trailer, data types and post-load reconciliation.
- Whole-file rejection: any failed pre-load check means nothing is loaded.
- A global concurrency limit on Dataflow jobs (default 1), enforced with GCS lock objects, so jobs fit the project's external-IP quota.
- An incident agent with deterministic policy, create-only retries, and IAM that cannot delete anything.

**Summary for managers:** files that partners drop into a bucket are checked, loaded into BigQuery and audited automatically, at roughly $0 when idle. When a file is bad or a load breaks, an agent explains why within minutes. It re-runs the file on its own only for temporary platform problems; everything else becomes a report for an engineer.

What it is not: it is not a streaming pipeline, not a general ETL framework (it appends rows; it does not transform or deduplicate them), and the agent never changes code, configuration or data. The `dags/` folder holds the earlier Airflow/Composer design. It is obsolete and not deployed (`infra/composer.tf:1-2`).

## Architecture

[Architecture diagram](diagrams/architecture.drawio)

[Interactive architecture view](diagrams/architecture.html)

| Component | Responsibility | Technology | Owning code |
|---|---|---|---|
| Landing bucket `<project>-dfa-landing` | Receives files under `landing/`. Processed files move to `archive/YYYY/MM/DD/` or `invalid/YYYY/MM/DD/`. `manual/` is for manual runs and has no notification. | GCS | `infra/storage.tf:5-14` |
| Topic `dfa-file-landed` | Receives `OBJECT_FINALIZE` notifications for the `landing/` prefix | Pub/Sub | `infra/pubsub.tf:1-25` |
| Eventarc trigger `dfa-file-landed` | Starts one workflow execution per message | Eventarc (Pub/Sub transport) | `infra/workflows.tf:33-58` |
| Workflow `dfa-ingest-file` | Orchestrates claim → register → slot → launch → poll → release → finalize → publish | Cloud Workflows YAML | `workflows/ingest_file.yaml:11-165` |
| Dataflow bucket `<project>-dfa-dataflow` | Holds `claims/`, `locks/`, `templates/`, `temp/` and `staging/` | GCS with lifecycle rules | `infra/storage.tf:16-45` |
| Dataflow Flex job | Validates, loads, reconciles and writes audit rows | Apache Beam 2.77 on Python 3.12 | `dataflow-job/pipeline/pipeline.py:90-126` |
| Artifact Registry `dfa-dataflow` | Stores the `gcs-to-bq:latest` launcher image | Docker image | `infra/workflows.tf:1-4`, `dataflow-job/Dockerfile:1-16` |
| `raw.customer`, `raw.user`, `raw.orders` | Target tables, with `_job_id`, `_source_file` and `_loaded_at` lineage columns | BigQuery, day-partitioned on `_loaded_at` | `infra/bigquery.tf:31-43` |
| `file_audit.job_metadata`, `validation_metadata`, `agent_incidents` | Run history, per-check results, agent decisions | BigQuery, day-partitioned on `created_at` | `infra/bigquery.tf:17-29`, `infra/agent.tf:10-20` |
| Topic `dfa-ingest-errors` and pull subscription `dfa-ingest-errors-sub` | One self-contained event per FAILED or REJECTED job, kept for 7 days | Pub/Sub | `infra/pubsub.tf:29-51` |
| `df-agent` | Investigates each error event and retries or reports | Python, LangGraph, Google Cloud client libraries | `df-agent/incident_core/graph.py:71-201` |
| Ollama `llama3.2` | Default LLM for every agent role. It can be switched to Anthropic per role. | Local Ollama at `localhost:11434` | `df-agent/config.yaml:9-17` |

**Boundaries and identities.**

- Everything except `df-agent` and Ollama runs in one GCP project, by default `bigquerytarun` in `europe-west2` (`Makefile:1-2`).
- Three service accounts are created (`infra/iam.tf:1-73`, `infra/agent.tf:5-86`):
  - `dfa-workflow` is the identity of the Eventarc trigger and the workflow. It can launch Dataflow, run BigQuery DML and act as `dfa-dataflow`.
  - `dfa-dataflow` is the worker identity. It has `bigquery.dataEditor` and `jobUser`.
  - `dfa-agent` is read-only, plus `storage.objectCreator` on the landing bucket, write access to `agent_incidents` only, and subscriber on the error subscription.
- Your gcloud user impersonates `dfa-agent` when the agent runs locally (`infra/agent.tf:79-86`, `df-agent/domains/dataflow_ingest/clients.py:21-27`).

Flow diagrams, each explained under [How it works](#how-it-works):

- Per-file ingestion sequence: `diagrams/ingest-file.drawio`
- Job status lifecycle: `diagrams/job-status.drawio`
- Beam pipeline dataflow: `diagrams/beam-pipeline.drawio`
- df-agent incident graph: `diagrams/agent-incident.drawio`

## Prerequisites

- **Python 3.12.** The Makefile creates its venvs with `python3.12` (`Makefile:49-52`, `Makefile:144-147`), and the launcher image is `python312-template-launcher-base` (`dataflow-job/Dockerfile:2`).
- **Terraform ≥ 1.5** with the `hashicorp/google` provider `~> 6.0` (`infra/versions.tf:1-10`). State is local in `infra/`.
- **Google Cloud SDK** (`gcloud`, `bq`), authenticated with `gcloud auth login` and `gcloud auth application-default login`.
- **Docker Desktop**, running, for `make docker`. On Apple Silicon the `linux/amd64` build is emulated and slow the first time (`Makefile:75-79`).
- **A GCP project** where you are Owner or Editor. Editor already includes `roles/artifactregistry.writer`, which the image push needs. Terraform enables the 12 APIs listed in `infra/apis.tf:1-24`.
- **Quota:** each Dataflow job uses about 2 external IPs (launcher VM + 1 worker). The default `IN_USE_ADDRESSES` quota of 4 is why `max_concurrent_jobs` defaults to 1 (`infra/variables.tf:36-40`).
- **For df-agent:** [Ollama](https://ollama.com) with the `llama3.2` model pulled, or an `ANTHROPIC_API_KEY` if you switch roles to Anthropic (`df-agent/config.yaml:9-17`).

## Installation and setup

From a clean checkout, run these in order. Everything is driven by the Makefile, and `PROJECT_ID`, `REGION` and `PREFIX` can be overridden per command, for example `make plan PROJECT_ID=my-project`.

```bash
# 1. Local Python environment for the pipeline and its tests
make venv            # .venv: apache-beam[gcp]==2.77.0, PyYAML, pytest, openpyxl
make test            # pipeline unit tests on DirectRunner, no GCP needed

# 2. Infrastructure (about 5 minutes)
make plan            # terraform init + plan
make gcs-setup       # terraform apply: buckets, Pub/Sub, Eventarc, workflow, BigQuery, IAM, Artifact Registry

# 3. Flex Template image and spec
make docker          # build linux/amd64 image, push to Artifact Registry, then `make template`

# 4. Verify
make gcs-check       # every resource exists and is ACTIVE; exits 1 on any failure
```

`make gcs-check` reports the Flex Template image and spec only as warnings (`scripts/gcs_check.sh:1-131`). Everything else must be OK.

Then set up the agent:

```bash
make agent-venv        # .venv-agent: LangGraph, LangChain, Google clients, reportlab
make agent-test        # policy matrix, signatures, brain coverage, graph scenarios (no GCP, no LLM)
make agent-brain-sync  # copy BQ schemas into df-agent/brain/schemas and check runbook coverage
ollama serve           # in another terminal, if Ollama is not already running
```

`make agent-brain-sync` prints `brain OK` when every signature runbook and every category playbook exists (`df-agent/incident_core/runner.py:178-190`).

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
| `FILE` | for `run-dataflow`, `run-local` | — | Mock file name or `gs://` URI | `Makefile:41` |
| `JOB_ID`, `INCIDENT`, `EXPECTED`, `NOTE` | for agent targets | — | Job or incident to replay, approve or save as an eval case | `Makefile:163-179` |
| `CONFIRM` | no | — | `yes` skips the `bq-clean-up` prompt | `Makefile:117-132` |

**Terraform variables** (`infra/variables.tf:1-46`, example in `infra/terraform.tfvars.example:1-3`):

| Variable / key | Required | Default | Purpose | Read at |
|---|---|---|---|---|
| `project_id` | yes | — | Project to deploy into | `infra/variables.tf:1-4` |
| `region` | no | `europe-west2` | Region | `infra/variables.tf:6-10` |
| `prefix` | no | `dfa` | Buckets are named `<project_id>-<prefix>-*` | `infra/variables.tf:12-16` |
| `audit_dataset` | no | `file_audit` | Audit dataset | `infra/variables.tf:18-22` |
| `target_dataset` | no | `raw` | Target dataset | `infra/variables.tf:24-28` |
| `agent_operator` | no | `""` (nobody) | Member granted Token Creator on `dfa-agent` | `infra/variables.tf:30-34` |
| `max_concurrent_jobs` | no | `1` | Number of Dataflow slots | `infra/variables.tf:36-40` |
| `target_tables` | no | `customer`, `user`, `orders` | Each needs `infra/bq/<name>.json` | `infra/variables.tf:42-46` |

**Workflow environment.** Terraform sets these as workflow user env vars: `DFA_REGION`, `DFA_AUDIT_DATASET`, `DFA_TARGET_DATASET`, `DFA_TEMPLATE_PATH`, `DFA_DATAFLOW_SA`, `DFA_DATAFLOW_BUCKET`, `DFA_MAX_CONCURRENT_JOBS` and `DFA_ERROR_TOPIC` (`infra/workflows.tf:15-25`). The workflow reads them with `sys.get_env()` (`workflows/ingest_file.yaml:14-24`). Change them through Terraform, not in the console.

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

**df-agent** (`df-agent/config.yaml:1-49`). `${VAR:-default}` placeholders are expanded from the environment, and relative directories resolve against `df-agent/` (`df-agent/incident_core/config.py:11-27`). The Makefile exports `DFA_PROJECT_ID`, `DFA_REGION`, `DFA_LANDING_BUCKET`, `DFA_DATAFLOW_BUCKET`, `DFA_ERROR_SUBSCRIPTION`, `DFA_AUDIT_DATASET`, `DFA_WORKFLOW_NAME`, `DFA_AGENT_SA` and `DFA_DATAFLOW_SA` from the Terraform outputs (`Makefile:35-38`). Other settings:

- `OLLAMA_BASE_URL` (default `http://localhost:11434`)
- `llm.default` and `llm.roles` (provider `ollama`, `anthropic` or `none` for rules-only mode)
- `features.generate_patches` (default `false`)
- `limits.evidence_rounds` (default 2)
- `impersonate_service_account` (empty means use your own ADC)

**Secrets.** The repository holds no secrets. GCP access is ADC plus service-account impersonation. `ANTHROPIC_API_KEY` is needed only if a role uses `provider: anthropic`; keep it in your shell, not in `config.yaml`.

## Usage

**Process files end to end.** Drop all mock files into `landing/` and watch them run:

```bash
make upload-mock     # gcloud storage cp mockdata/*.csv mockdata/*.xlsx gs://<landing>/landing/
make executions      # recent workflow executions and their state
make errors          # peek at up to 10 error events (not acked)
```

To ingest a real file, upload `<entity>_<YYYYMMDD>.csv` to `gs://<project>-dfa-landing/landing/`. Its last line must be the trailer `T|<record_count>|<YYYYMMDD>`.

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

**Run the pipeline without the workflow:**

```bash
make run-dataflow FILE=customer_20261015.csv            # copies to manual/ (no trigger) and runs on Dataflow
make run-local FILE=mockdata/customer_20261015.csv      # DirectRunner, writes to real BigQuery
```

Manual runs use `trigger_type=MANUAL`. The pipeline's MERGE inserts the `job_metadata` row, and the file is not moved. `run-dataflow` bypasses the slot locks, so do not run it during a bulk upload (`Makefile:88-112`).

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

Each run prints an investigation trace and ends with a line like `RESULT <id> | status AWAITING_HUMAN | root cause DATA_QUALITY (0.95) | executed nothing | ...` (`df-agent/incident_core/runner.py:44-70`). Each incident's outputs land in `df-agent/actions/<incident_id>/`: `report.md`, `report.pdf`, `evidence.json`, `review.md`, `state.json`, `trace.md` and `trace.jsonl` (`df-agent/incident_core/report/writer.py:23-76`). The folder is git-ignored.

## How it works

### Per-file ingestion (Cloud Workflows)

[Per-file ingestion sequence](diagrams/ingest-file.drawio)

[Interactive per-file ingestion view](diagrams/ingest-file.html)

Eventarc delivers the Pub/Sub message as a CloudEvent to workflow `dfa-ingest-file`. The `main` block runs these steps:

1. **Filter.** It returns `skipped` for any event type other than `OBJECT_FINALIZE`, and for any object outside `landing/` or ending in `/` (`workflows/ingest_file.yaml:32-54`).
2. **Claim.** `claim_delivery` creates `claims/<bucket>/<object>@<generation>` in the Dataflow bucket with `ifGenerationMatch=0`. A second delivery gets HTTP 412 and exits before writing anything (`workflows/ingest_file.yaml:168-200`). Claims expire after 8 days through a bucket lifecycle rule, which outlives Pub/Sub's 7-day redelivery window (`infra/storage.tf:33-42`).
3. **Register.** `register_job` generates a UUID `job_id` and counts earlier attempts for the same `file_path` and generation. It inserts `IN_PROGRESS`, or `DUPLICATE_SKIPPED` if that generation already succeeded. All parameters are STRING and are cast in SQL (`workflows/ingest_file.yaml:203-287`). The `bq_query` helper calls the BigQuery REST API with named parameters (`workflows/ingest_file.yaml:730-776`).
4. **Acquire a slot.** `acquire_slot` tries to create `locks/dataflow-slot-<n>` for each `n < DFA_MAX_CONCURRENT_JOBS`. If every slot is held, it sleeps 60 s and tries again. A lock older than 7,200 s is deleted as abandoned (`workflows/ingest_file.yaml:290-381`).
5. **Launch and poll.** It calls `flexTemplates:launch` with `maxWorkers: 1` and the pipeline parameters (`workflows/ingest_file.yaml:89-123`). `wait_for_dataflow` then polls `jobs.get` every 60 s, up to 60 times, before returning `POLL_TIMEOUT` (`workflows/ingest_file.yaml:406-438`). Any launch or poll exception becomes `LAUNCH_OR_POLL_FAILED` (`workflows/ingest_file.yaml:134-140`).
6. **Release.** The lock is deleted whatever the outcome (`workflows/ingest_file.yaml:142-150`, `workflows/ingest_file.yaml:384-403`).
7. **Finalize.** `finalize_job` reads the status Dataflow wrote. It forces `FAILED` with `error_code = 'DATAFLOW_JOB_FAILED'` if the job did not end `JOB_STATE_DONE`, or if the row is still `IN_PROGRESS`. It copies the file to `archive/` (SUCCESS) or `invalid/` (anything else), deletes the original, and records `final_file_path`. For FAILED or REJECTED it publishes the error event. A FAILED job also fails the execution (`workflows/ingest_file.yaml:441-567`).
8. **Publish.** `publish_failure` assembles the job row, failed checks (with details truncated to 2,000 chars), up to the tail of Dataflow error messages, console links and ready-to-run audit SQL. It publishes all of that to `dfa-ingest-errors` with attributes `status`, `error_code`, `job_id`, `job_name` and `file_name` (`workflows/ingest_file.yaml:570-727`).

### Job status lifecycle

[Job status lifecycle](diagrams/job-status.drawio)

[Interactive job status view](diagrams/job-status.html)

`file_audit.job_metadata.status` moves from `IN_PROGRESS` to one of four terminal values:

- The workflow writes `IN_PROGRESS` or `DUPLICATE_SKIPPED` at registration (`workflows/ingest_file.yaml:231-242`).
- The Dataflow `FinalizeJobMetadata` MERGE writes the outcome. It is `REJECTED` if any pre-load check is blocking (error code = the first failed check's name), `FAILED` / `RECON_MISMATCH` if reconciliation failed or did not run, and `SUCCESS` otherwise (`dataflow-job/pipeline/transforms/audit.py:74-110`).
- The workflow can still override the result to `FAILED` afterwards (`workflows/ingest_file.yaml:462-481`).

`job_metadata` is written only with DML (INSERT, UPDATE or MERGE), never with streaming inserts, so rows can always be updated (`dataflow-job/pipeline/transforms/audit.py:113-117`). For a manual run the MERGE inserts the row itself, with `service_name = 'dataflow'` (`dataflow-job/pipeline/transforms/audit.py:171-179`).

A FAILED or REJECTED file is not final. Re-uploading a fixed file, or a df-agent `RETRY_FILE` copy from `invalid/` back to `landing/`, creates a new object generation and therefore a new job.

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

**Listener.** `runner.py listen` pulls one message at a time and extends its ack deadline to 600 s while the graph runs. It acks after the run, and on an exception resets the deadline to 120 s so the message is redelivered. A message it cannot parse is saved to `actions/_unparseable/` and acked (`df-agent/incident_core/runner.py:79-119`). `replay` rebuilds the event from the audit tables (`df-agent/domains/dataflow_ingest/pack.py:54-64`).

**Graph** (`df-agent/incident_core/graph.py:163-201`):

- **intake** skips incidents already recorded as handled. It closes `SUCCESS`, `IN_PROGRESS` and `DUPLICATE_SKIPPED` jobs as `NO_INCIDENT` and sets the severity: SEV2 for FAILED, SEV3 for REJECTED, one level higher after 3 or more failures in 24 h (`df-agent/incident_core/graph.py:76-96`, `df-agent/domains/dataflow_ingest/pack.py:66-90`).
- **audit_agent** and **gcp_agent** run in parallel. Each runs a fixed set of default lookups, and an LLM may choose up to 4 extra read-only tools only when the investigator asks (`df-agent/incident_core/agents/gatherer.py:65-148`). Tools are typed functions with fixed queries, not a shell (`df-agent/domains/dataflow_ingest/tools_gcp.py:62-226`).
- **log_analyst** builds a deterministic error digest, matches `brain/signatures.yaml`, and runs verification calls for the top signature. A CONFIRMED verification raises its confidence to at least 0.97; a REFUTED one caps it at 0.5 (`df-agent/incident_core/agents/log_analyst.py:24-103`).
- **investigator** sets the root cause to one of TRANSIENT_INFRA, DATA_QUALITY, CODE_BUG, CONFIG or UNKNOWN. A signature at or above `signature_override_confidence` (0.85) decides the category, and the LLM only writes the explanation. Without the LLM it falls back to the signature, or to UNKNOWN. It can request more evidence up to `limits.evidence_rounds` times (`df-agent/incident_core/agents/investigator.py:50-153`).
- **action_planner** maps the category to its playbook actions (`df-agent/brain/playbooks.yaml:1-40`) and fills in parameters. For `RETRY_FILE` these are a copy from `invalid/...` back to the original `landing/...` name (`df-agent/domains/dataflow_ingest/pack.py:105-118`). A `PROPOSE_CODE_CHANGE` action routes through **code_reviewer** (`git apply --check` only).
- **action_reviewer** runs `PolicyEngine.evaluate` first, which is authoritative. Unknown types, forbidden patterns (`delete`, `drop`, `truncate`, `rm`, `mv`, `--force`, …) and bad parameters are REJECTED, and non-executable types become REPORT. `RETRY_FILE` is AUTO_APPROVED only when all of these hold: the job FAILED, the category is TRANSIENT_INFRA, confidence ≥ 0.7, fewer than 2 retries in 24 h, the source exists, the destination does not exist, and no stuck ingest jobs remain. Otherwise it is NEEDS_HUMAN if fewer than 5 retries were made (`df-agent/brain/policies.yaml:12-57`, `df-agent/incident_core/policy.py:64-92`). The LLM review may only downgrade AUTO_APPROVED to NEEDS_HUMAN (`df-agent/incident_core/agents/reviewers.py:81-110`).
- **executor** runs only AUTO_APPROVED actions. `RETRY_FILE` is a GCS rewrite with `if_generation_match=0` on the landing bucket and has no delete path (`df-agent/domains/dataflow_ingest/executor.py:8-30`). The final status is AUTO_RESOLVED or AWAITING_HUMAN (`df-agent/incident_core/graph.py:109-135`).
- **report_writer** and **record** write `actions/<id>/`. They append a row to `file_audit.agent_incidents`, mirrored to `.state/incidents.jsonl`. If the BigQuery write fails, the local mirror still records the incident (`df-agent/incident_core/store.py:84-112`).

**Human approval.** `make agent-approve` re-checks each NEEDS_HUMAN action against fresh policy facts, asks `Execute? [y/N]`, then records `APPROVED_BY_HUMAN` (`df-agent/incident_core/runner.py:126-175`).

### Error handling, retries and concurrency

- **Pub/Sub at-least-once delivery:** absorbed by the claim object and the `DUPLICATE_SKIPPED` check (`workflows/ingest_file.yaml:56-71`).
- **Eventarc:** retries if it cannot create the execution. Inside the workflow, only `jobs.get` uses `http.default_retry` (`workflows/ingest_file.yaml:415-423`).
- **Moving the file:** a 404 while moving means it was already moved, so it is ignored (`workflows/ingest_file.yaml:516-523`). A failure to publish the error event is logged and does not change the outcome (`workflows/ingest_file.yaml:543-550`).
- **Concurrency:** limited to `DFA_MAX_CONCURRENT_JOBS` Dataflow jobs. Files queue inside their own workflow executions, so `duration_seconds` includes the time spent waiting for a slot.
- **Agent:** handles one incident at a time, and uses a SQLite checkpointer per run (`df-agent/incident_core/graph.py:204-209`).

## Deployment and operations

- **Infrastructure and workflow:** `make gcs-setup` (`terraform apply -auto-approve`, `Makefile:66-67`). The workflow YAML is deployed by Terraform from `workflows/ingest_file.yaml` (`infra/workflows.tf:11`), so edit the YAML and rerun `make gcs-setup` to release a change. There is no CI pipeline in the repository.
- **Pipeline code or contracts:** `make docker` rebuilds and pushes `gcs-to-bq:latest`, then `make template` rewrites `gs://<dataflow bucket>/templates/gcs_to_bq.json` (`Makefile:75-86`). Run `make template` alone after changing only `metadata.json`. The image tag is always `latest`, so rolling back means rebuilding from an earlier commit.
- **Environments:** one project per `PROJECT_ID`. Terraform state is local (`infra/*.tfstate`, git-ignored), so run Terraform from one machine, or move the state to a remote backend before sharing.
- **Health check:** `make gcs-check` is read-only and exits 1 on any failure (`Makefile:69-70`).
- **Logs and history:**
  - Workflow executions log errors only (`call_log_level = LOG_ERRORS_ONLY`, `infra/workflows.tf:12`).
  - `make executions` lists recent runs, and each `job_metadata` row stores the execution id in `service_revision`.
  - Dataflow job logs and errors are in the Dataflow console. The error event carries the console URL.
  - Agent traces are in `df-agent/actions/<id>/trace.md`.
- **Alerts:** no Cloud Monitoring alerts are defined. The error topic and `df-agent` are the alerting path.
- **Scaling:** raise `max_concurrent_jobs` (for example `make gcs-setup MAX_JOBS=2`) only after raising the regional `IN_USE_ADDRESSES` quota. Each job needs about 2 IPs.
- **Backup and restore:** none is configured. Buckets use `force_destroy` and datasets use `delete_contents_on_destroy`, so `make gcs-destroy` deletes every file and row (`infra/storage.tf:7-14`, `infra/bigquery.tf:1-15`).
- **Cost:** about $0 when idle. About $0.02 per file in Dataflow, and about 45 Workflows steps per file (`project.md` §12).

> [!WARNING]
> `make gcs-destroy` and `make bq-clean-up` delete data irreversibly. `gcs-destroy` force-deletes both buckets and both datasets. `bq-clean-up` truncates every audit and target table.

## Troubleshooting

| Symptom | Likely cause | How to confirm | Fix |
|---|---|---|---|
| `make gcs-setup` fails on the Eventarc trigger or the Pub/Sub token-creator binding in a new project | The Eventarc or Pub/Sub service agent does not exist yet | The error names `service-<number>@gcp-sa-pubsub` or Eventarc | Wait a minute and rerun `make gcs-setup`. It continues where it stopped. |
| `No terraform outputs found - run 'make gcs-setup' first.` | No local Terraform state | `terraform -chdir=infra output` is empty | Run `make gcs-setup` from this checkout (`Makefile:117-118`) |
| `make docker` fails | Docker Desktop not running, or no `artifactregistry.writer` | `docker info`, and check the IAM roles on your account | Start Docker, or grant the role |
| A file stays `IN_PROGRESS` for a long time | It is waiting for a Dataflow slot (one job at a time, about 6 min each) | `make executions` shows several ACTIVE executions, and `gs://<dataflow bucket>/locks/` holds a slot object | Wait. Locks older than 2 h are cleared automatically (`workflows/ingest_file.yaml:339-358`). |
| Workflow execution returns `skipped: true, reason: duplicate delivery` | Pub/Sub redelivered the same message | The claim object exists under `claims/` | Nothing to do. This is by design. |
| Status `DUPLICATE_SKIPPED` | That file generation already loaded successfully | An earlier `SUCCESS` row has the same `file_path` and `file_generation` | Upload the file again to create a new generation, if you really want to reload it |
| Status `REJECTED`, error code `FILE_NAME`, `HEADER`, `TRAILER`, `DELIMITER`, `DATA_TYPE` or `CONTRACT_MATCH` | The file breaks its contract | `SELECT * FROM file_audit.validation_metadata WHERE job_id = '<id>'` shows the expected and actual values and up to 10 sample rows | Fix the file and re-upload it to `landing/`. See `df-agent/brain/runbooks/<check>.md`. |
| Status `FAILED` with `UnicodeDecodeError` | The file is not UTF-8 (e.g. Latin-1) | The Dataflow errors in the error event. df-agent matches the `encoding_error` signature as CODE_BUG (`df-agent/brain/signatures.yaml:99-105`). | Re-encode the file as UTF-8, or change the pipeline to decode it, then `make docker` |
| Status `FAILED` with `QUOTA_EXCEEDED` / `RESOURCE_EXHAUSTED` | More concurrent jobs than the IP quota allows (e.g. `MAX_JOBS=4`) | Dataflow job messages; the agent's `get_quota` evidence (`df-agent/brain/signatures.yaml:61-66`) | df-agent retries automatically (TRANSIENT_INFRA). Lower `MAX_JOBS` or raise the quota. |
| Status `FAILED`, error code `DATAFLOW_JOB_FAILED`, reason `LAUNCH_OR_POLL_FAILED` | Template spec or image missing, or a permission problem | `status_reason` holds the launch error. `make gcs-check` warns about the template. | `make docker`, then retry with `make agent-approve INCIDENT=<id>` or re-upload |
| Status `FAILED`, error code `RECON_MISMATCH` | Rows in BigQuery ≠ file rows ≠ trailer count | `recon_row_count` details in `validation_metadata` | See `df-agent/brain/runbooks/RECON_MISMATCH.md` |
| df-agent logs `LLM gave no valid ... falling back` or `LLM output unavailable` | Ollama is not running, or the small model returned invalid JSON | `curl http://localhost:11434` | Start `ollama serve`. Classification still works from signatures (`df-agent/incident_core/agents/investigator.py:122-131`). |
| df-agent logs `could not record incident in BigQuery (kept locally)` | No permission on `agent_incidents`, or impersonation failed | `df-agent/.state/incidents.jsonl` has the row | Check `AGENT_OPERATOR` and rerun `make gcs-setup` (`df-agent/incident_core/store.py:91-98`) |
| `make agent-approve` prints `BLOCKED by policy now` | Facts changed: the destination already exists, the source is gone, or the retry limit is reached | The printed blockers | Resolve the blocker manually; the agent will not override policy (`df-agent/incident_core/runner.py:140-146`) |
| Error events pile up in `dfa-ingest-errors-sub` | The agent is not running | `make errors` | `make agent-listen`. Messages are kept for 7 days. |

## Source map

| Path | Responsibility |
|---|---|
| `Makefile` | Every operator command: infrastructure, image, runs, clean-up, agent |
| `infra/` | Terraform: APIs, buckets, Pub/Sub, Eventarc, workflow, BigQuery, IAM, agent identity |
| `infra/bq/*.json` | BigQuery table schemas, including `agent_incidents` |
| `workflows/ingest_file.yaml` | Per-file orchestration, deployed by Terraform |
| `dataflow-job/main.py`, `pipeline/pipeline.py` | Pipeline entry point and graph |
| `dataflow-job/pipeline/checks.py` | Validation rules and type conversion |
| `dataflow-job/pipeline/transforms/` | File checks, parse, load, reconcile, audit writers |
| `dataflow-job/contracts/*.yaml` | Per-entity data contracts |
| `dataflow-job/tests/` | Beam `TestPipeline` unit tests (`make test`) |
| `df-agent/incident_core/` | Domain-agnostic agent core: graph, agents, policy, signatures, store, report, CLI |
| `df-agent/domains/dataflow_ingest/` | Domain pack: event parsing, audit and GCP tools, `RETRY_FILE` executor |
| `df-agent/brain/` | Signatures, playbooks, policies, runbooks, system prompt |
| `df-agent/tests/` | Policy, signature, error and graph scenario tests (`make agent-test`) |
| `scripts/gcs_check.sh` | Read-only resource health check |
| `mockdata/` | Sample good and bad files, and `generate.py` |
| `dags/`, `infra/composer.tf` | Obsolete Airflow/Composer design, not deployed |

Where to start common changes:

- **Add an entity:**
  1. Add `dataflow-job/contracts/<entity>.yaml` and `infra/bq/<entity>.json`.
  2. Append the entity to `target_tables` (`infra/variables.tf:42-46`).
  3. Run `make gcs-setup` and `make docker`.
- **Add a validation rule:** write it in `dataflow-job/pipeline/checks.py`, emit it from `BuildContentChecks` or `ValidateFileStructure`, and add a test in `dataflow-job/tests/test_checks.py`.
- **Change orchestration:** edit `workflows/ingest_file.yaml`, then run `make gcs-setup`.
- **Teach the agent a new failure:** add a signature to `df-agent/brain/signatures.yaml` and a runbook under `df-agent/brain/runbooks/`, then run `make agent-brain-sync` and `make agent-test`.
- **Allow a new automatic action:** add it to `action_types`, `param_rules` and `auto_approve` in `df-agent/brain/policies.yaml`, and implement it in `DataflowIngestPack.execute` (`df-agent/domains/dataflow_ingest/pack.py:133-136`).

## Testing

```bash
make test         # dataflow-job/tests: ValidateAndParse and the checks, DirectRunner, no GCP
make agent-test   # df-agent/tests: policy matrix, signatures, brain coverage, graph scenarios with fakes
make agent-eval   # offline replay of recorded incidents; exits non-zero below 100% accuracy
```

The agent tests use fake clients and no LLM (`df-agent/tests/fakes.py:1-134`). `agent-eval` reports root-cause accuracy over `df-agent/evals/cases/` (`df-agent/incident_core/runner.py:251-257`).
