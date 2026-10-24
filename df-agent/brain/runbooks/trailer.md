# Runbook: trailer check failed
**Category:** DATA_QUALITY · **Status:** REJECTED

## How to confirm
`get_validation_results`: the `trailer` failed_checks show which rule failed:
- `TRAILER_FORMAT`: last line isn't `T|<count>|<YYYYMMDD>` (missing or malformed). `details.last_line` shows what was there.
- `TRAILER_COUNT`: `trailer_count` ≠ `data_rows`. The file may be truncated (rows lost in transfer), or the producer miscounted.
- `TRAILER_DATE`: trailer date ≠ file-name date.

## Action
`PROPOSE_DATA_FIX`. For TRAILER_COUNT, stress that the file **may be incomplete**: the sender should resend the full
file, not just edit the trailer.
