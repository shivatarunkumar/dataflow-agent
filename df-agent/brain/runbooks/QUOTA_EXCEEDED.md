# Runbook: QUOTA_EXCEEDED / launcher VM could not start
**Category:** TRANSIENT_INFRA (usually) · **Status:** FAILED

## Symptoms
- Dataflow error: `Failed to start the launcher VM ... RESOURCE_EXHAUSTED ... QUOTA_EXCEEDED 'IN_USE_ADDRESSES'` (or `CPUS`).
- `job_metadata.error_code = DATAFLOW_JOB_FAILED`, with dataflow state `JOB_STATE_FAILED` shortly after launch.

## How to confirm
1. `get_dataflow_errors`: look for `QUOTA_EXCEEDED`, `IN_USE_ADDRESSES`, `CPUS`, `ZONE_RESOURCE_POOL_EXHAUSTED`.
2. `get_quota` for `IN_USE_ADDRESSES` and `CPUS`: is usage near the limit now?
3. `get_recent_failures` for the contract: are many jobs failing at the same time (burst upload)?

## Likely causes
- Several Dataflow jobs ran at once (each uses about 2 external IPs). This is transient: retry when capacity frees up.
- `max_concurrent_jobs` set above what the quota allows. This is CONFIG.
- Zone capacity shortage (`ZONE_RESOURCE_POOL_EXHAUSTED`). Transient.

## Action
- If transient: `RETRY_FILE` (auto when policy allows).
- If the same error repeats after 2 retries, or the quota limit is lower than one job needs: `PROPOSE_CONFIG_CHANGE`
  (request an `IN_USE_ADDRESSES` / `CPUS` quota increase in europe-west2, or lower `max_concurrent_jobs`).
