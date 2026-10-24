# Runbook: file_name check failed
**Category:** DATA_QUALITY · **Status:** REJECTED

## How to confirm
`get_validation_results`: `FILE_NAME_PATTERN` (doesn't match `<entity>_<YYYYMMDD>.csv`) or `FILE_NAME_DATE` (invalid date).
Common cases: wrong extension (`.xlsx`), dashes in the date (`2026-10-19`), extra suffixes (`customer_id_200.csv`).

## Action
`PROPOSE_DATA_FIX`: give the correct name, e.g. `customer_20261019.csv`. The file must be re-uploaded under that name;
renaming in place isn't an agent action. A non-CSV file must be exported as CSV first.
