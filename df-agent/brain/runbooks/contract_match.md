# Runbook: contract_match check failed
**Category:** DATA_QUALITY · **Status:** REJECTED

## How to confirm
`get_validation_results`: no contract for the file-name prefix. `check_config.known_prefixes` lists the valid prefixes.

## Action
- If the file was sent to the wrong place or misnamed: `PROPOSE_DATA_FIX` (rename to a known prefix, or route it elsewhere).
- If it's a genuinely new feed: `PROPOSE_CODE_CHANGE` (report-only). Add `dataflow-job/contracts/<entity>.yaml`,
  `infra/bq/<entity>.json`, and the table name in the `target_tables` Terraform variable.
