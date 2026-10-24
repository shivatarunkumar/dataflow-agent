# Runbook: BigQuery schema mismatch on load
**Category:** CODE_BUG · **Status:** FAILED

## Symptoms
Load job errors: `No such field: <col>`, `Provided Schema does not match Table`, `Cannot convert value`, or
`too many errors ... JSON`.

## How to confirm
The error digest's message names the field. Compare the contract columns (`dataflow-job/contracts/<entity>.yaml`) with
`infra/bq/<entity>.json` and the pipeline's lineage columns (`_job_id`, `_source_file`, `_loaded_at`).

## Action
`PROPOSE_CODE_CHANGE`: align the contract, the BigQuery schema and `Contract.bq_schema()`. A schema change also needs
`make gcs-setup`, and a pipeline change needs `make docker`. Then re-upload the file. Never retry automatically.
