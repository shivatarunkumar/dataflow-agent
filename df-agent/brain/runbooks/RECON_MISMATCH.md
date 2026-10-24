# Runbook: RECON_MISMATCH
**Category:** CODE_BUG (default) · **Status:** FAILED

## Symptoms
`recon_row_count` check FAIL: file rows, trailer count and BigQuery rows for the job_id differ, although every pre-load check passed.

## How to confirm
1. `get_validation_results`: the `recon_row_count` details show `rows_in_file`, `trailer_count` and `rows_in_bq`.
2. `get_file_history`: was the same file loaded twice (rows from another job_id)? Recon counts per job_id, so that alone shouldn't cause it.

## Likely causes
- `rows_in_bq < rows_in_file`: the load was partial, or some rows were filtered after validation (bug in `ParseRecord` or the gate).
- `rows_in_bq > rows_in_file`: duplicate load within the same job (retried bundle).

## Action
- `PROPOSE_CODE_CHANGE` pointing at `dataflow-job/pipeline/transforms/{parse,load,recon}.py`.
- Never retry automatically. Loaded rows exist for this job_id, and a human must decide how to clean them up
  (the agent must never delete table data).
