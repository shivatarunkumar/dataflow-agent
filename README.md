# dataflow-agent

A file-ingestion pipeline on Google Cloud with an incident-response agent.

CSV files dropped into a GCS landing bucket are validated and loaded into BigQuery by an Apache Beam job on Dataflow,
orchestrated by Airflow (Cloud Composer 3). Every run and every validation check is recorded in BigQuery audit tables.
When a file fails, **df-agent** investigates it from the Airflow run down to the Dataflow error. It writes an incident
report and a ServiceNow change request draft, and retries the file itself when the failure is clearly transient.

```
GCS landing/ ──▶ Pub/Sub ──▶ Airflow (Composer 3) ──▶ Dataflow (Beam) ──▶ BigQuery raw.* + file_audit.*
                                                          │
                              FAILED / REJECTED ──▶ Pub/Sub dfa-ingest-errors ──▶ df-agent ──▶ report.pdf + change.md
```

## Quick start
```bash
make gcs-setup      # infrastructure (Composer stays off)
make airflow-up     # start Airflow (~25 min; billed ~$0.80–1.00/hour while up)
gcloud storage cp mockdata/customer_20261015.csv gs://bigquerytarun-dfa-landing/landing/
make agent-listen   # investigate failures as they arrive
make airflow-down   # stop Airflow when done
```
All commands, in order, with expected results: **[COMMANDS.md](COMMANDS.md)**.

## Repository
| Path | What |
|---|---|
| `infra/` | Terraform: GCS, Pub/Sub, Composer 3, BigQuery, IAM, Artifact Registry, agent identity |
| `dags/` | Airflow DAGs: `pubsub_file_listener` (one run per file) and `gcs_to_bq_dataflow` (register → Dataflow → finalize) |
| `dataflow-job/` | Beam pipeline (Flex Template image) and per-entity data contracts |
| `df-agent/` | Incident agent (LangGraph, local Ollama by default) |
| `mockdata/` | Sample good and bad files |
| `scripts/gcs_check.sh` | Resource health check (`make gcs-check`) |

## Documentation
- [project.md](project.md): pipeline design, flow, checks, tables, error events, cost
- [df-agent/df-agent.md](df-agent/df-agent.md): agent design, safety model, report and change request
- [COMMANDS.md](COMMANDS.md): deploy, test and tear-down commands

## Prerequisites
`gcloud` (authenticated to project `bigquerytarun`), Terraform, Python 3.12, Docker (for `make docker`), and
Ollama with `llama3.2` for the agent's LLM steps (it falls back to rules without it).
