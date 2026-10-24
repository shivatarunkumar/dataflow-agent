# Runbook: Flex template or image missing
**Category:** CONFIG · **Status:** FAILED

## How to confirm
- `get_workflow_execution`: launch error mentioning `templates/gcs_to_bq.json`, or the image.
- `get_gcs_object` on the template path: does it exist?

## Action
`PROPOSE_CONFIG_CHANGE`: run `make docker` (builds and pushes the image and writes the template spec), then re-upload the file.
