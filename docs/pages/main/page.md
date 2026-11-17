---
title: Dataflow-Agent
repository: https://github.com/shivatarunkumar/dataflow-agent.git
revision: f8d708f10ddffad661614e26672a5b49625a69ea
generated: 2026-11-08
---

# Dataflow-Agent

## What is this?

Dataflow-Agent takes data files that people send us, checks them, and loads the good ones into our database (BigQuery).

If something goes wrong, a software helper called the **agent** works out why and writes a short report, so nobody has to dig through logs.

## How it works

![How a file moves through the system](architecture.png)

1. **A file arrives.** Someone puts a CSV file in our storage folder (the "landing bucket").
2. **Airflow notices.** Airflow is the scheduler that runs the process. It checks for new files every 2 minutes.
3. **Dataflow checks the file.** It looks at the name, the columns, the row count and the values.
4. **Good files are loaded.** The data goes into BigQuery. If any check fails, nothing from that file is loaded.
5. **Everything is recorded.** Each file and each check is written to a log table, so we can always see what happened.
6. **Problems go to the agent.** The agent finds the cause and writes a report. It only retries a file by itself if the problem was temporary.

## What can happen to a file

| Result | What it means | Who acts |
|---|---|---|
| **Success** | The data is loaded | Nobody |
| **Rejected** | Something is wrong with the file | The person who sent it fixes it and sends it again |
| **Failed** | Something went wrong on our side | The agent retries it, or an engineer fixes the cause |
| **Skipped** | We already loaded this exact file | Nobody |

## Status today

> [!WARNING]
> **Not fully working yet.** We can't currently build the software package that Dataflow needs, because of a problem with a Google component it's based on. Until that is fixed, files can't be processed from start to finish.

## Good to know

- **Cost:** Airflow costs about $1 per hour while it's switched on, even when idle, so we only switch it on when needed. Everything else costs almost nothing when idle.
- **Files are safe while Airflow is off.** New files wait for up to 7 days and are processed when it's switched back on.
- **The agent can't break things.** It can't delete anything or change code, settings or data.

## Where to go next

| I want to... | Go to |
|---|---|
| Set it up, run it, or switch it off | [Dataflow-Agent Runbook](https://shivatarunkumar.atlassian.net/wiki/spaces/~712020e41a8275c9054106bd81e51b882e6221/pages/688169/Dataflow-Agent+Runbook) |
| Fix something that went wrong | [Dataflow-Agent Playbook](https://shivatarunkumar.atlassian.net/wiki/spaces/~712020e41a8275c9054106bd81e51b882e6221/pages/720970/Dataflow-Agent+Playbook) |

## Words you might see

| Word | Meaning |
|---|---|
| **Bucket** | A storage folder in Google Cloud |
| **BigQuery** | The database where the data ends up |
| **Airflow** | The scheduler that runs each step in order |
| **Composer** | Google's service that hosts Airflow for us |
| **Dataflow** | The Google service that reads, checks and loads each file |
| **Agent** | The software helper that investigates problems |
| **Contract** | The rules a file must follow, such as its column names |
