# Runbook: data_type check failed
**Category:** DATA_QUALITY · **Status:** REJECTED

## How to confirm
`get_validation_results`: the `data_type` details give `rule_counts` (e.g. `customer_id.NOT_NULL: 1`,
`signup_date.TYPE: 1`) and up to 10 sample rows with their errors.

## Action
`PROPOSE_DATA_FIX`: one bullet per failing rule, giving the column, the problem, the number of rows, an example value and
the expected format (dates `YYYY-MM-DD` unless the contract sets a format, e.g. orders `DD/MM/YYYY`; booleans true/false).
If a rule looks too strict for valid business data, also propose a contract change (report-only).
