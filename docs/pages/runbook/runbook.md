---
title: Dataflow-Agent Runbook
repository: https://github.com/shivatarunkumar/dataflow-agent.git
revision: f8d708f10ddffad661614e26672a5b49625a69ea
generated: 2026-11-08
---

# Dataflow-Agent Runbook

This page shows how to set up and run Dataflow-Agent, step by step. If something has gone wrong, use the [Dataflow-Agent Playbook](https://shivatarunkumar.atlassian.net/wiki/spaces/~712020e41a8275c9054106bd81e51b882e6221/pages/720970/Dataflow-Agent+Playbook) instead.

Run every command from the main project folder.

## Before you start

You need these on your computer:

- Google Cloud command-line tools (`gcloud`), signed in to the project `bigquerytarun`
- Terraform
- Python 3.12
- Docker Desktop
- Ollama, which powers the agent

## First-time setup

You only do this once.

**1. Install and test the code.** This is free.

```bash
make venv
make test
make dags-test
make agent-venv
make agent-test
```

**2. Create the cloud resources.** This is free.

```bash
make plan
make gcs-setup
```

**3. Build the software package.**

```bash
make docker
```

> [!WARNING]
> This step currently fails with an `invalid tar header` error. See [Image build fails](https://shivatarunkumar.atlassian.net/wiki/spaces/~712020e41a8275c9054106bd81e51b882e6221/pages/720970/Dataflow-Agent+Playbook) in the Playbook.

**4. Check that everything is there.**

```bash
make gcs-check
```

A warning that "Composer is down" is normal at this point.

## Each time you use it

**1. Switch Airflow on.** This takes about 25 minutes, and **billing starts here**.

```bash
make airflow-up
```

The first time only, open Airflow and sign in, then give yourself admin access:

```bash
make airflow-ui
make airflow-admin
```

**2. Start the agent.** Use two extra terminal windows, one for each command, and leave them running.

```bash
ollama serve
```

```bash
make agent-listen
```

**3. Send a file.**

```bash
gcloud storage cp customer_20261015.csv gs://bigquerytarun-dfa-landing/landing/
```

Airflow picks it up within 2 minutes. Each file takes about 6 minutes, and files run one at a time.

**4. Check the result.**

```bash
make airflow-runs
```

**5. Switch Airflow off when you finish.** This takes about 10 minutes, and **billing stops**. Your data and files are kept.

```bash
make airflow-down
```

> [!WARNING]
> Always do this step. Airflow costs about $1 per hour for as long as it's on, even if nothing is running.

## What a file must look like

| Rule | Example |
|---|---|
| The name is the type of data, then the date | `customer_20261015.csv` |
| It's a CSV file | Excel files are rejected |
| The first line has the right column names | They must match the contract |
| The last line says how many rows there are, and the date | See the example below |

For a file with 5 rows, dated 15 October 2026, the last line is:

```text
T|5|20261015
```

Today we accept three types of data: `customer`, `user` and `orders`.

## Where to see what happened

Run this in BigQuery to see the latest files and their results:

```sql
SELECT file_name, status, rows_in_file, rows_loaded, created_at
FROM `bigquerytarun.file_audit.job_metadata`
ORDER BY created_at DESC LIMIT 20;
```

When a file fails, the agent puts these in `df-agent/actions/<job id>/`:

- `report.pdf`: what happened, and why
- `change.md`: a ready-made ServiceNow change request, if a fix is needed

## How it works, in pictures

### The journey of one file

![The journey of one file](ingest-file.png)

Airflow picks up the file, records it, asks Dataflow to process it, waits, and then files it away. If there was a problem, it tells the agent.

### The result of a file

![The result of a file](job-status.png)

Every file ends as success, rejected, failed or skipped. A failed or rejected file can be sent again, and it's treated as a new file.

### How a file gets checked

![How a file gets checked](beam-pipeline.png)

The file is checked first and loaded only if every check passes. Afterwards, the number of rows loaded is compared with the number in the file.

### How the agent investigates

![How the agent investigates](agent-incident.png)

The agent collects the evidence, works out the cause, and decides what to do. A fixed set of safety rules has the final say on any action.

## Making changes

| What changed | What to run |
|---|---|
| Airflow steps (the `dags` folder) | `make dags-test`, then `make dags-deploy` |
| File checks or rules (the `dataflow-job` folder) | `make test`, then `make docker` |
| Cloud resources (the `infra` folder) | `make plan`, then `make gcs-setup` |

## Clearing test data

> [!WARNING]
> These commands permanently delete data.

| Command | What it deletes |
|---|---|
| `make bq-clean-up` | All rows in the log tables and data tables |
| `make gcs-destroy` | Everything: all cloud resources, files and tables |

## Technical details

The full engineering documentation, including every setting, is in the code repository: [dataflow-agent on GitHub](https://github.com/shivatarunkumar/dataflow-agent/tree/feat-df-airflow-new).
