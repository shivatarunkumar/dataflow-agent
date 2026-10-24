# Runbook: PERMISSION_DENIED (missing IAM permission)
**Category:** CONFIG · **Status:** FAILED

## Symptoms
At the **bottom** of a Dataflow worker traceback (the frames above it are Beam internals):
```
RuntimeError: apitools.base.py.exceptions.HttpForbiddenError: HttpError accessing <https://bigquery.googleapis.com/...>
  ... 'status': '403' ...
  "message": "Access Denied: Dataset <project>:raw: Permission bigquery.tables.create denied on dataset <project>:raw"
  "reason": "accessDenied", "status": "PERMISSION_DENIED"
```
Also: `PERMISSION_DENIED` / `403` from GCS (`storage.objects.*`), or from the Dataflow API at launch (in the workflow
execution error).

## How to confirm (the agent does this automatically)
1. The error digest names the **permission** (e.g. `bigquery.tables.create`) and the **resource** (e.g. dataset `raw`).
2. `get_dataflow_job`: the **worker service account** the job ran as (`dfa-dataflow@…`).
3. `check_permission(member, permission, dataset)`: the account's project roles and dataset roles, and whether any of
   them includes the permission. **CONFIRMED** = no role grants it.

## Why it happens
A role was removed from, or never granted to, the service account; or the job ran as a different account.
`bigquery.tables.create` / `bigquery.tables.updateData` come from `roles/bigquery.dataEditor`, which the Dataflow SA needs
on the target dataset (temp tables + loads).

## Action
- `PROPOSE_CONFIG_CHANGE`: grant the role **in Terraform** (`infra/iam.tf`), then `make gcs-setup`. Hand-made grants are
  lost or drift.
- `RETRY_FILE` after the fix, **with human approval** (`make agent-approve`). Never auto-retry: the same permission error
  repeats until IAM is fixed.
- Not a data problem and not a code bug. The file and the pipeline code are fine.
