---
title: Dataflow-Agent Playbook
repository: https://github.com/shivatarunkumar/dataflow-agent.git
revision: f8d708f10ddffad661614e26672a5b49625a69ea
generated: 2026-11-08
---

# Dataflow-Agent Playbook

Use this page when something has gone wrong. Find what you're seeing, then follow the steps.

## Start here

1. **Open the report.** The agent writes one for each problem in `df-agent/actions/<job id>/report.pdf`. It tells you what failed and why.
2. **Find your problem** in the table below.
3. **Follow the steps**, then [send the file again](#send-the-file-again) if the steps say so.

Not getting any reports? See [The agent isn't working](#the-agent-isnt-working).

## What are you seeing?

| What you see | What it means | Go to |
|---|---|---|
| File status is **Rejected** | The file has a mistake | [The file has a mistake](#the-file-has-a-mistake) |
| `UnicodeDecodeError` | The file is saved in the wrong text format | [Wrong text format](#wrong-text-format) |
| `invalid tar header` when building | The software package can't be built | [Image build fails](#image-build-fails) |
| `404` or `Unable to open template file` | The software package is missing | [Software package missing](#software-package-missing) |
| `403` or `Access Denied` | An account doesn't have permission | [Permission problem](#permission-problem) |
| `QUOTA_EXCEEDED` | Too many jobs ran at once | [Too many jobs](#too-many-jobs) |
| `Not found: Table` | A database table is missing | [Missing table](#missing-table) |
| Column or schema error | The file's rules and the table don't match | [Rules and table don't match](#rules-and-table-dont-match) |
| `zombie`, `killed` or `SIGTERM` | Airflow restarted during the job | [Airflow restarted](#airflow-restarted) |
| `AirflowTaskTimeout` | The job took more than 60 minutes | [Job took too long](#job-took-too-long) |
| `RECON_MISMATCH` | Rows loaded don't match the file | [Row counts don't match](#row-counts-dont-match) |
| An error in our code | A bug needs fixing | [Bug in the code](#bug-in-the-code) |
| Nothing happens after sending a file | The file wasn't picked up | [File isn't picked up](#file-isnt-picked-up) |
| Anything else | The cause isn't clear | [Get help](#get-help) |

> [!NOTE]
> If Ollama isn't running, the agent can wrongly blame the code for a `404` error. Follow [Software package missing](#software-package-missing) instead.

## Send the file again

Do this only after you've fixed the problem. Airflow must be on.

If the report says a retry is waiting for approval:

```bash
make agent-approve INCIDENT=<job id>
```

Otherwise, copy the file back. The report tells you where the file is now.

```bash
gcloud storage cp gs://bigquerytarun-dfa-landing/invalid/YYYY/MM/DD/<file> gs://bigquerytarun-dfa-landing/landing/<file>
```

**It worked if** the newest result for the file says `SUCCESS` and the rows loaded equal the rows in the file.

## Problems with the file

These are for the person who sent the file to fix.

### The file has a mistake

Nothing was loaded. Don't send the same file again. Ask the sender to fix it and send a new one.

| Failed check | What's wrong | What to ask for |
|---|---|---|
| `file_name` | The name is wrong | A name like `customer_20261015.csv` |
| `contract_match` | We don't recognise this type of file | The right name, or ask an engineer to add the new type |
| `header` | Column names are wrong | The correct column names, in the correct order |
| `delimiter` | Columns aren't separated properly | The file saved again with commas between columns |
| `trailer` | The last line is wrong or missing | The **whole file again**, because some rows may be missing |
| `data_type` | Some values are empty or in the wrong format | Corrected values; the report lists them |

### Wrong text format

The file isn't saved as UTF-8. Ask the sender to save it as UTF-8 and send it again. Sending the same file again will fail the same way.

## Problems on our side

### Image build fails

Docker can't unpack a Google component that our software package is based on. Several versions have failed, and **no fix is confirmed yet**.

Try these one at a time and stop when one works:

```bash
make docker
```

```bash
make docker BASE_IMAGE=gcr.io/dataflow-templates-base/python312-template-launcher-base:flex_templates_base_image_release_20260803_RC00
```

```bash
make docker BASE_IMAGE=gcr.io/dataflow-templates-base/python311-template-launcher-base:latest
```

When one works, tell an engineer which one, so it can be made the default.

### Software package missing

Dataflow can't find the package it needs to start.

```bash
make docker
make gcs-check
```

Then [send the file again](#send-the-file-again).

### Permission problem

An account is missing a permission. The report names the account and the permission.

1. Ask an engineer to add the permission in the `infra/iam.tf` file. Don't add it by hand in the console, because it can get lost.
2. Run `make plan`, then `make gcs-setup`.
3. [Send the file again](#send-the-file-again).

### Too many jobs

Too many jobs ran at the same time. The agent normally retries by itself, so first check whether it already worked.

If it keeps happening, ask an engineer to raise the Google Cloud limit, or run one job at a time with `make gcs-setup MAX_JOBS=1`.

### Missing table

A database table doesn't exist. Run `make gcs-setup` to create it, then [send the file again](#send-the-file-again).

### Rules and table don't match

The columns in the file's rules are different from the columns in the table. An engineer needs to make them match, then run `make gcs-setup` and `make docker`. Then [send the file again](#send-the-file-again).

### Airflow restarted

Airflow restarted while it was working. This is temporary and the agent normally retries by itself. Check that the retry worked.

### Job took too long

The job ran for more than 60 minutes and was stopped. Look at the job in Dataflow to see why it was slow. Send the file again only if the cause was temporary.

### Row counts don't match

> [!WARNING]
> **Don't send the file again yet.** Some rows may already be loaded, so you could end up with duplicates.

Ask an engineer and the data owner to decide how to clean up. An engineer must find the cause before the file is sent again.

### Bug in the code

An engineer needs to fix the code. The report shows where the error is. After the fix:

| Bug is in | Run |
|---|---|
| The `dags` folder | `make dags-test`, then `make dags-deploy` |
| The `dataflow-job` folder | `make test`, then `make docker` |

Then [send the file again](#send-the-file-again).

### File isn't picked up

| Check | Fix |
|---|---|
| Is Airflow on? Run `make gcs-check` | If not, run `make airflow-up` |
| Is the file directly inside `landing/`? | If not, send it again to the right place |
| Are other files ahead of it? | Wait: files run one at a time, about 6 minutes each |

### The agent isn't working

| What you see | Fix |
|---|---|
| No reports at all | Run `make agent-listen` |
| `Connection refused` | Run `ollama serve` |
| `'change' is undefined`, or the same problem repeating | Stop the agent with Ctrl-C and run `make agent-listen` again |

## Unexpected costs

Airflow costs about $1 per hour while it's on. To switch it off:

```bash
make airflow-down
```

Your data and files are kept.

## Get help

Contact **Data Platform Engineering** on **#data-platform-ops**. Share the job id, the `report.pdf`, and what you've already tried.

Code and setting changes need a ServiceNow change request. The agent drafts one for you in `change.md`, next to the report.
