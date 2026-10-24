# Runbook: delimiter check failed
**Category:** DATA_QUALITY · **Status:** REJECTED

## How to confirm
`get_validation_results`: the `delimiter` details show `expected_delimiter`, `detected_delimiter`,
`header_field_count` vs `expected_field_count`, and sample bad rows.
- `HEADER_DELIMITER`: the whole file uses another delimiter (e.g. `|` instead of `,`). The header check usually fails too.
- `ROW_FIELD_COUNT` only: some rows have extra or missing fields, often unquoted commas inside values.

## Action
`PROPOSE_DATA_FIX`: re-export with the contract delimiter, or quote values that contain it. Quote the sample rows.
