# Runbook: DATAFLOW_JOB_FAILED (generic Dataflow failure)
**Category:** depends on the error text · **Status:** FAILED

## How to confirm
1. `get_dataflow_errors`: read the first error. It usually names the cause.
2. `get_logs` preset `dataflow_job`: WARNING+ entries around the failure time.
3. `get_workflow_execution`: did the launch itself fail (`LAUNCH_OR_POLL_FAILED`) or time out (`POLL_TIMEOUT`)?

## Classify
| Evidence | Category |
|---|---|
| Python `Traceback`, `ValueError`, `KeyError`, … from the pipeline code (`pipeline/…py`) | CODE_BUG |
| `UNAVAILABLE`, `DEADLINE_EXCEEDED`, `An internal error occurred`, HTTP 5xx, connection reset | TRANSIENT_INFRA |
| `PERMISSION_DENIED`, 403 | CONFIG (see PERMISSION_DENIED) |
| Template or image not found | CONFIG (see TEMPLATE_MISSING) |
| `POLL_TIMEOUT` (job ran > 60 min) | TRANSIENT_INFRA if the job later succeeded, otherwise UNKNOWN |

## Action
- TRANSIENT_INFRA → `RETRY_FILE`.
- CODE_BUG → `PROPOSE_CODE_CHANGE`: name the file and function from the traceback, describe the fix, and add a unit
  test in `dataflow-job/tests/`. After the fix: `make docker`, then re-upload the file.
- Otherwise → `ESCALATE` with the error text.
