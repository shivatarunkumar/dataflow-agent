# Runbook: header check failed
**Category:** DATA_QUALITY · **Status:** REJECTED

## How to confirm
`get_validation_results`: the `header` details show `expected`, `actual`, `missing` and `unexpected`, plus `order_mismatch`.

## Action
`PROPOSE_DATA_FIX`: list the missing and unexpected columns, and the exact header line expected (contract column
order, contract delimiter). The data owner fixes the header and re-uploads the file. If the source system changed its
format on purpose, also propose a contract change (`dataflow-job/contracts/<entity>.yaml` + the target table schema),
which is report-only.
