PROJECT_ID ?= bigquerytarun
REGION     ?= europe-west2
PREFIX     ?= dfa

# Who may impersonate the df-agent SA when running the agent locally (defaults to your gcloud account).
AGENT_OPERATOR ?= $(shell gcloud config get-value account 2>/dev/null)

TF      := terraform -chdir=infra
# Dataflow jobs allowed at once. Keep 1 (fits the 4-IP quota); MAX_JOBS=4 deliberately triggers QUOTA_EXCEEDED for agent tests.
MAX_JOBS ?= 1

TF_VARS := -var project_id=$(PROJECT_ID) -var region=$(REGION) -var prefix=$(PREFIX) \
           -var max_concurrent_jobs=$(MAX_JOBS) \
           $(if $(AGENT_OPERATOR),-var agent_operator=user:$(AGENT_OPERATOR),)
VENV    := .venv
PYTHON  := $(abspath $(VENV)/bin/python)
AGENT_VENV   := .venv-agent
AGENT_PYTHON := $(abspath $(AGENT_VENV)/bin/python)

# Values from terraform outputs (resolved lazily, only by targets that need them).
tf_out          = $(shell $(TF) output -raw $(1) 2>/dev/null)
LANDING_BUCKET  = $(call tf_out,landing_bucket)
DATAFLOW_BUCKET = $(call tf_out,dataflow_bucket)
TEMPLATE_PATH   = $(call tf_out,template_path)
IMAGE_URI       = $(call tf_out,image_uri)
DATAFLOW_SA     = $(call tf_out,dataflow_sa)
WORKFLOW_NAME   = $(call tf_out,workflow_name)
AUDIT_DATASET   = $(call tf_out,audit_dataset)
TARGET_DATASET  = $(call tf_out,target_dataset)
TARGET_TABLES   = $(call tf_out,target_tables)
ERROR_SUB       = $(call tf_out,error_subscription)
AGENT_SA        = $(call tf_out,agent_sa)

# Environment for df-agent (expanded into df-agent/config.yaml).
AGENT_ENV = DFA_PROJECT_ID=$(PROJECT_ID) DFA_REGION=$(REGION) DFA_LANDING_BUCKET=$(LANDING_BUCKET) \
            DFA_DATAFLOW_BUCKET=$(DATAFLOW_BUCKET) DFA_ERROR_SUBSCRIPTION=$(ERROR_SUB) \
            DFA_AUDIT_DATASET=$(AUDIT_DATASET) DFA_WORKFLOW_NAME=$(WORKFLOW_NAME) DFA_AGENT_SA=$(AGENT_SA) \
            DFA_DATAFLOW_SA=$(DATAFLOW_SA)

# FILE=customer_20261015.csv uploads mockdata/<FILE> to manual/ (no Pub/Sub trigger); FILE=gs://... is used as-is.
FILE_URI = $(if $(filter gs://%,$(FILE)),$(FILE),gs://$(LANDING_BUCKET)/manual/$(notdir $(FILE)))

.PHONY: help venv test mockdata tf-init plan gcs-setup gcs-check gcs-destroy docker template run-dataflow run-local upload-mock executions bq-clean-up errors \
        agent-venv agent-test agent-listen agent-once agent-replay agent-approve agent-brain-sync agent-eval agent-eval-add agent-graph

help:
	@grep -E '^[a-z-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-16s %s\n", $$1, $$2}'

venv: ## Create .venv (Python 3.12) with pipeline + test dependencies
	python3.12 -m venv $(VENV)
	$(VENV)/bin/pip install --upgrade pip
	$(VENV)/bin/pip install -r dataflow-job/requirements.txt -r requirements-dev.txt

test: ## Run pipeline unit tests (DirectRunner, no GCP)
	cd dataflow-job && $(PYTHON) -m pytest -q tests

mockdata: ## Regenerate mock files in mockdata/
	$(PYTHON) mockdata/generate.py

tf-init:
	$(TF) init

plan: tf-init ## terraform plan
	$(TF) plan $(TF_VARS)

gcs-setup: tf-init ## Create all resources (incl. workflow + Eventarc trigger); run `make docker` after
	$(TF) apply -auto-approve $(TF_VARS)

gcs-check: ## Verify every resource exists and is active (exit 1 on any failure)
	PROJECT_ID=$(PROJECT_ID) REGION=$(REGION) bash scripts/gcs_check.sh

gcs-destroy: tf-init ## Destroy all resources (buckets/datasets are force-deleted)
	$(TF) destroy -auto-approve $(TF_VARS)

docker: ## Build the flex template image locally, push it to Artifact Registry, then write the template spec
	gcloud auth configure-docker $(REGION)-docker.pkg.dev --quiet
	docker build --platform linux/amd64 -t $(IMAGE_URI) dataflow-job
	docker push $(IMAGE_URI)
	$(MAKE) template

template: ## Write the flex template spec (GCS) pointing at the pushed image
	gcloud dataflow flex-template build $(TEMPLATE_PATH) \
		--image $(IMAGE_URI) \
		--sdk-language PYTHON \
		--metadata-file dataflow-job/metadata.json \
		--project $(PROJECT_ID)

run-dataflow: ## Launch on Dataflow from local: make run-dataflow FILE=customer_20261015.csv | FILE=gs://...
	@test -n "$(FILE)" || (echo "Usage: make run-dataflow FILE=<mockdata file name | gs:// uri>"; exit 1)
	$(if $(filter gs://%,$(FILE)),,gcloud storage cp mockdata/$(notdir $(FILE)) $(FILE_URI))
	cd dataflow-job && $(PYTHON) main.py \
		--runner DataflowRunner \
		--project $(PROJECT_ID) \
		--region $(REGION) \
		--job_name ingest-manual-$(shell date +%Y%m%d%H%M%S) \
		--temp_location gs://$(DATAFLOW_BUCKET)/temp \
		--staging_location gs://$(DATAFLOW_BUCKET)/staging \
		--service_account_email $(DATAFLOW_SA) \
		--setup_file ./setup.py \
		--max_num_workers 1 \
		--input_file $(FILE_URI) \
		--trigger_type MANUAL

run-local: ## DirectRunner against real BigQuery: make run-local FILE=mockdata/customer_20261015.csv
	@test -n "$(FILE)" || (echo "Usage: make run-local FILE=<local path | gs:// uri>"; exit 1)
	cd dataflow-job && $(PYTHON) main.py \
		--runner DirectRunner \
		--project $(PROJECT_ID) \
		--region $(REGION) \
		--temp_location gs://$(DATAFLOW_BUCKET)/temp \
		--input_file $(if $(filter gs://%,$(FILE)),$(FILE),$(abspath $(FILE))) \
		--trigger_type MANUAL

upload-mock: ## Drop all mock files into landing/ (triggers Pub/Sub -> Eventarc -> Workflows -> Dataflow)
	gcloud storage cp mockdata/*.csv mockdata/*.xlsx gs://$(LANDING_BUCKET)/landing/

bq-clean-up: ## TRUNCATE audit + target tables (asks to confirm; CONFIRM=yes skips the prompt)
	@test -n "$(AUDIT_DATASET)" || { echo "No terraform outputs found - run 'make gcs-setup' first."; exit 1; }
	@tables="$(AUDIT_DATASET).job_metadata $(AUDIT_DATASET).validation_metadata"; \
	for t in $$(echo "$(TARGET_TABLES)" | tr ',' ' '); do tables="$$tables $(TARGET_DATASET).$$t"; done; \
	echo "This will delete ALL rows from these tables in $(PROJECT_ID):"; \
	for t in $$tables; do echo "  $$t"; done; \
	if [ "$(CONFIRM)" != "yes" ]; then \
		read -p "Type 'yes' to continue: " answer; \
		[ "$$answer" = "yes" ] || { echo "Aborted."; exit 1; }; \
	fi; \
	for t in $$tables; do \
		echo "Truncating $$t"; \
		bq --project_id $(PROJECT_ID) --location $(REGION) query --use_legacy_sql=false --quiet \
			"TRUNCATE TABLE \`$(PROJECT_ID).$$t\`" > /dev/null || exit 1; \
	done; \
	echo "All tables truncated."

errors: ## Peek at error events (not acked: they stay on the subscription for the future agent)
	gcloud pubsub subscriptions pull $(ERROR_SUB) --project $(PROJECT_ID) --limit 10 \
		--format 'json(message.attributes, message.data.decode(base64))'

executions: ## List recent workflow executions (one per landed file)
	gcloud workflows executions list $(WORKFLOW_NAME) --location $(REGION) --project $(PROJECT_ID) --limit 20 \
		--format 'table(name.basename():label=EXECUTION, state, startTime, endTime)'

# --- df-agent (incident-response agent, see df-agent/df-agent.md) ------------------------------------

agent-venv: ## Create .venv-agent with df-agent dependencies (separate from the Beam venv)
	python3.12 -m venv $(AGENT_VENV)
	$(AGENT_VENV)/bin/pip install --upgrade pip
	$(AGENT_VENV)/bin/pip install -r df-agent/requirements.txt

agent-test: ## df-agent unit + scenario tests (no GCP, no LLM)
	cd df-agent && $(AGENT_PYTHON) -m pytest -q tests

agent-brain-sync: ## Copy BQ schemas into the agent brain and check runbook/playbook coverage
	mkdir -p df-agent/brain/schemas
	cp infra/bq/job_metadata.json infra/bq/validation_metadata.json infra/bq/agent_incidents.json df-agent/brain/schemas/
	cd df-agent && $(AGENT_PYTHON) -m incident_core.runner brain-check

agent-listen: ## Run the agent: handle error events one at a time (Ctrl-C to stop)
	cd df-agent && $(AGENT_ENV) $(AGENT_PYTHON) -m incident_core.runner listen

agent-once: ## Handle at most one pending error event, then exit
	cd df-agent && $(AGENT_ENV) $(AGENT_PYTHON) -m incident_core.runner once

agent-replay: ## Re-investigate a job without Pub/Sub: make agent-replay JOB_ID=<job_id>
	@test -n "$(JOB_ID)" || { echo "Usage: make agent-replay JOB_ID=<job_id>"; exit 1; }
	cd df-agent && $(AGENT_ENV) $(AGENT_PYTHON) -m incident_core.runner replay --ref $(JOB_ID)

agent-eval: ## Replay recorded incidents (df-agent/evals/cases) and report root-cause accuracy (offline)
	cd df-agent && $(AGENT_ENV) $(AGENT_PYTHON) -m incident_core.runner eval

agent-eval-add: ## Save an incident as an eval case: make agent-eval-add INCIDENT=<id> EXPECTED=CONFIG [NOTE="..."]
	@test -n "$(INCIDENT)" -a -n "$(EXPECTED)" || { echo "Usage: make agent-eval-add INCIDENT=<id> EXPECTED=<CATEGORY>"; exit 1; }
	cd df-agent && $(AGENT_PYTHON) -m incident_core.runner eval-add --incident $(INCIDENT) --expected $(EXPECTED) --note "$(NOTE)"

agent-graph: ## Draw the agent workflow to df-agent/graph.mmd (offline); PNG=1 also renders graph.png via mermaid.ink
	cd df-agent && $(AGENT_PYTHON) -m incident_core.runner graph $(if $(PNG),--png,)

agent-approve: ## Approve pending non-code actions (e.g. retries): make agent-approve INCIDENT=<job_id>
	@test -n "$(INCIDENT)" || { echo "Usage: make agent-approve INCIDENT=<incident_id>"; exit 1; }
	cd df-agent && $(AGENT_ENV) $(AGENT_PYTHON) -m incident_core.runner approve --incident $(INCIDENT)
