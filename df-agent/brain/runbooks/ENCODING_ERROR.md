# Runbook: Encoding error (UnicodeDecodeError)
**Category:** CODE_BUG · **Status:** FAILED

## Symptoms
Dataflow errors / logs end with
`UnicodeDecodeError: 'utf-8' codec can't decode byte 0x.. in position N` raised from
`apache_beam/io/textio.py ... read_records` → `coders.py ... value.decode('utf-8')`.
The file passed the header/trailer/name checks (they read only the edges and tolerate bad bytes), then the job
crashed while reading data rows. Beam retries the bundle 4 times, so the job ends `JOB_STATE_FAILED`.

## How to confirm
1. `get_dataflow_errors` / `get_logs` preset `dataflow_job`: the exception line above.
2. The byte value tells the likely source encoding: `0xe9` = `é`, `0xfc` = `ü` in **Latin-1 / Windows-1252**.

## Root cause
The pipeline assumes UTF-8 and has no handling for other encodings. A bad byte crashes the whole job instead of
producing a clean REJECTED with a validation record. It's a code defect (robustness), triggered by a non-UTF-8 file.

## Action
- `PROPOSE_CODE_CHANGE` (report-only):
  - add an `encoding` field to the contract (default `utf-8`);
  - read the file with that encoding: `ReadFromText(..., coder=...)` with a custom coder, or read bytes and decode
    in `ParseRecord` with `errors="strict"` inside try/except;
  - send undecodable rows to a new tagged output, and add an `encoding` validation check (FAIL → REJECTED) so the
    job finishes cleanly with an audit record;
  - add a unit test with a Latin-1 file (e.g. `mockdata/customer_20261021.csv`).
- `PROPOSE_DATA_FIX`: until the code is fixed, the sender should re-export the file as UTF-8.
- Never retry automatically: the same bytes fail the same way.
