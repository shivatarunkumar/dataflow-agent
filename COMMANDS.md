# Handy commands: deploy, test, tear down

Project `bigquerytarun`, region `europe-west2`. Run everything from the repo root. `make help` lists every target.

> **Cost:** Composer (Airflow) is billed **~$0.80–1.00 per hour while it exists**, idle or not. Everything else is ~$0
> when idle. Always finish a session with `make airflow-down`. A typical 3-hour session costs about **$2–4**.

## 1. Local tests (free)
```bash
make dags-test                        # DAG logic + error-event tests
make test                             # Dataflow pipeline tests
make agent-venv && make agent-test    # (re)create the agent venv, incl. Chromium; then agent tests
```

## 2. Create / update the infrastructure (free)
Make sure no files are mid-processing first.
```bash
make plan        # review the changes; Composer is NOT created here
make gcs-setup   # apply; Composer keeps its current state (up stays up, down stays down)
make gcs-check   # health check; "Composer is down" is a warning, not a failure
make docker      # build + push the Flex Template image and write the spec (after changing dataflow-job/)
```
If `gcs-setup` fails on the `composer.ServiceAgentV2Ext` binding, the Composer service agent isn't created yet. Wait a
minute and run it again.

## 3. Budget alert (once)
```bash
gcloud billing budgets create --billing-account=<BILLING_ACCOUNT_ID> --display-name=dfa-guard --budget-amount=5GBP \
  --threshold-rule=percent=0.5 --threshold-rule=percent=1.0
```

## 4. Start Airflow (~25 min, billing starts)
```bash
make airflow-up          # creates Composer and uploads dags/
make airflow-ui          # prints the Airflow UI URL: open it and sign in once
make airflow-admin       # gives your account Admin in Airflow (new users get Viewer)
make dags-deploy         # re-upload dags/ after changing a DAG (picked up within ~1 min)
```

## 5. Run test files (~6 min each; one Dataflow job at a time)
```bash
LANDING=gs://bigquerytarun-dfa-landing/landing/
gcloud storage cp mockdata/customer_20261015.csv  $LANDING   # SUCCESS
gcloud storage cp mockdata/user_20261016.csv      $LANDING   # REJECTED (header)
gcloud storage cp mockdata/customer_20261021.csv  $LANDING   # FAILED: Dataflow crash (Latin-1 encoding)
make airflow-test-launch-failure FILE=customer_20261015.csv   # FAILED in Airflow: missing template, no Dataflow job
make upload-mock                                              # all mock files (~75 min end to end)

make airflow-runs        # DAG runs and their states (or watch the Airflow UI)
make errors              # peek at error events waiting for the agent (not acked)
```
Files dropped while Composer is down wait on Pub/Sub (7 days) and are processed after `make airflow-up`.

## 6. Run the agent (another terminal)
```bash
ollama serve                                   # if Ollama isn't already running (model: llama3.2)
make agent-listen                              # handle error events as they arrive (Ctrl-C to stop)
make agent-once                                # or: handle one pending event, then exit
make agent-replay JOB_ID=<job_id>              # re-investigate a job (no Pub/Sub needed)
make agent-approve INCIDENT=<job_id>           # approve a pending retry after fixing the cause
```
Output per incident in `df-agent/actions/<job_id>/`:
- `report.pdf`: the incident report
- `change.md`: ServiceNow change request draft, or why none is needed
- `details/`: `report.md`, `evidence.json`, `trace.md`, `llm/`, … (supporting files)

What to expect:
| Test file | Failing layer | Root cause |
|---|---|---|
| `customer_20261021.csv` | DATAFLOW | CODE_BUG (encoding) |
| `user_20261016.csv` | DATA | DATA_QUALITY (header) |
| launch-failure test | AIRFLOW | CONFIG (template not found) |

## 7. Check results in BigQuery
```sql
SELECT file_name, status, error_code, rows_in_file, rows_loaded, final_file_path, created_at
FROM `bigquerytarun.file_audit.job_metadata` ORDER BY created_at DESC LIMIT 20;

SELECT file_name, check_name, validation_result, summary
FROM `bigquerytarun.file_audit.validation_metadata` ORDER BY created_at DESC LIMIT 50;
```
```bash
make bq-clean-up         # empty the audit + target tables (asks to confirm)
```

## 8. Stop Airflow (~10 min, billing stops)
```bash
make airflow-down        # deletes ONLY the Composer environment; data, buckets, Pub/Sub stay
```

## Other
```bash
make run-dataflow FILE=customer_20261015.csv   # launch Dataflow directly, bypassing Airflow (don't mix with a bulk upload)
make airflow-up MAX_JOBS=4                     # quota test: 4 parallel jobs -> QUOTA_EXCEEDED -> agent auto-retry
make gcs-destroy                               # delete everything Terraform created
```
