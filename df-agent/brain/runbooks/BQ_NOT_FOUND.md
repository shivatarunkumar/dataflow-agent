# Runbook: BigQuery resource not found
**Category:** CONFIG · **Status:** FAILED

## Symptoms
`Not found: Table <project>:<dataset>.<table>` / `Not found: Dataset ...` / reason `notFound` / HTTP 404, usually from the
load step (`LoadToTargetTable/WriteToBigQuery`) or reconciliation.

## How to confirm
The error digest's resource names the missing table or dataset. Compare it with the contract's `target_table` and with
`infra/bigquery.tf` (`target_tables` variable).

## Action
`PROPOSE_CONFIG_CHANGE`: recreate it via Terraform (`make gcs-setup`), or fix the contract's `target_table`. Then
`RETRY_FILE` with human approval.
