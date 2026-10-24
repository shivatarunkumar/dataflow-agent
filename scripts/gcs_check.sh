#!/usr/bin/env bash
# Verifies every resource created by infra/ exists and is active. Read-only.
# Usage: make gcs-check   (reads terraform outputs; PROJECT_ID / REGION come from the Makefile)
set -uo pipefail

TF="terraform -chdir=infra"
out() { $TF output -raw "$1" 2>/dev/null; }

PROJECT_ID="${PROJECT_ID:-$(out project_id)}"
REGION="${REGION:-$(out region)}"
if [[ -z "$(out landing_bucket)" ]]; then
  echo "No terraform outputs found - run 'make gcs-setup' first." >&2
  exit 1
fi

LANDING_BUCKET=$(out landing_bucket)
DATAFLOW_BUCKET=$(out dataflow_bucket)
TOPIC=$(out topic)
AUDIT_DATASET=$(out audit_dataset)
TARGET_DATASET=$(out target_dataset)
TARGET_TABLES=$(out target_tables)
DATAFLOW_SA=$(out dataflow_sa)
WORKFLOW_SA=$(out workflow_sa)
WORKFLOW_NAME=$(out workflow_name)
EVENTARC_TRIGGER=$(out eventarc_trigger)
ARTIFACT_REPO=$(out artifact_repo)
IMAGE_URI=$(out image_uri)
TEMPLATE_PATH=$(out template_path)
SERVICES=$(out services)

PASSED=0
FAILED=0
WARNED=0
GREEN=$'\033[32m'; RED=$'\033[31m'; YELLOW=$'\033[33m'; BOLD=$'\033[1m'; RESET=$'\033[0m'

section() { printf "\n%s%s%s\n" "$BOLD" "$1" "$RESET"; }

# check <label> <command...>: OK if the command exits 0.
check() {
  local label=$1; shift
  if "$@" >/dev/null 2>&1; then
    printf "  %sOK  %s  %s\n" "$GREEN" "$RESET" "$label"; PASSED=$((PASSED + 1))
  else
    printf "  %sFAIL%s  %s\n" "$RED" "$RESET" "$label"; FAILED=$((FAILED + 1))
  fi
}

# expect <label> <expected> <actual>: OK if equal.
expect() {
  local label=$1 expected=$2 actual=$3
  if [[ "$actual" == "$expected" ]]; then
    printf "  %sOK  %s  %s (%s)\n" "$GREEN" "$RESET" "$label" "$actual"; PASSED=$((PASSED + 1))
  else
    printf "  %sFAIL%s  %s (expected %s, got '%s')\n" "$RED" "$RESET" "$label" "$expected" "$actual"; FAILED=$((FAILED + 1))
  fi
}

# warn_check: like check but a failure is only a warning (things deployed after gcs-setup).
warn_check() {
  local label=$1; shift
  if "$@" >/dev/null 2>&1; then
    printf "  %sOK  %s  %s\n" "$GREEN" "$RESET" "$label"; PASSED=$((PASSED + 1))
  else
    printf "  %sWARN%s  %s\n" "$YELLOW" "$RESET" "$label"; WARNED=$((WARNED + 1))
  fi
}

echo "${BOLD}Checking resources in project ${PROJECT_ID} (${REGION})${RESET}"

section "APIs"
ENABLED=$(gcloud services list --enabled --project "$PROJECT_ID" --format='value(config.name)' 2>/dev/null)
for svc in ${SERVICES//,/ }; do
  check "$svc" grep -qx "$svc" <<<"$ENABLED"
done

section "Cloud Storage"
for bucket in "$LANDING_BUCKET" "$DATAFLOW_BUCKET"; do
  check "gs://$bucket" gcloud storage buckets describe "gs://$bucket" --project "$PROJECT_ID"
done
NOTIFICATIONS=$(gcloud storage buckets notifications list "gs://$LANDING_BUCKET" --project "$PROJECT_ID" --format=json 2>&1)
check "notification gs://$LANDING_BUCKET -> $TOPIC (OBJECT_FINALIZE, landing/)" \
  bash -c 'grep -q "topics/$1" <<<"$2" && grep -q "OBJECT_FINALIZE" <<<"$2" && grep -q "landing/" <<<"$2"' _ "$TOPIC" "$NOTIFICATIONS"
if ! grep -q "topics/$TOPIC" <<<"$NOTIFICATIONS"; then
  echo "        gcloud returned:"
  head -n 8 <<<"$NOTIFICATIONS" | sed 's/^/          /'
fi

section "Pub/Sub"
check "topic $TOPIC" gcloud pubsub topics describe "$TOPIC" --project "$PROJECT_ID"
check "error topic $(out error_topic)" gcloud pubsub topics describe "$(out error_topic)" --project "$PROJECT_ID"
check "error subscription $(out error_subscription)" gcloud pubsub subscriptions describe "$(out error_subscription)" --project "$PROJECT_ID"
TOPIC_SUBS=$(gcloud pubsub topics list-subscriptions "$TOPIC" --project "$PROJECT_ID" --format='value(.)' 2>/dev/null)
check "Eventarc subscription attached to $TOPIC" test -n "$TOPIC_SUBS"

section "Eventarc / Workflows"
TRIGGER_JSON=$(gcloud eventarc triggers describe "$EVENTARC_TRIGGER" --location "$REGION" --project "$PROJECT_ID" --format=json 2>/dev/null)
check "trigger $EVENTARC_TRIGGER exists" test -n "$TRIGGER_JSON"
check "trigger $EVENTARC_TRIGGER targets workflow $WORKFLOW_NAME" grep -q "workflows/$WORKFLOW_NAME" <<<"$TRIGGER_JSON"
# Triggers report readiness in conditions; a non-ready trigger shows a condition with code != OK.
check "trigger $EVENTARC_TRIGGER ready (no failing conditions)" \
  bash -c 'test -n "$1" && ! grep -Eq "\"code\": \"(FAILED_PRECONDITION|NOT_FOUND|PERMISSION_DENIED|INTERNAL)\"" <<<"$1"' _ "$TRIGGER_JSON"
expect "workflow $WORKFLOW_NAME state" "ACTIVE" \
  "$(gcloud workflows describe "$WORKFLOW_NAME" --location "$REGION" --project "$PROJECT_ID" --format='value(state)' 2>/dev/null)"

section "BigQuery"
for ds in "$AUDIT_DATASET" "$TARGET_DATASET"; do
  check "dataset $ds" bq --project_id "$PROJECT_ID" show --format=none "$PROJECT_ID:$ds"
done
for tbl in job_metadata validation_metadata; do
  check "table $AUDIT_DATASET.$tbl" bq --project_id "$PROJECT_ID" show --format=none "$PROJECT_ID:$AUDIT_DATASET.$tbl"
done
for tbl in ${TARGET_TABLES//,/ }; do
  check "table $TARGET_DATASET.$tbl" bq --project_id "$PROJECT_ID" show --format=none "$PROJECT_ID:$TARGET_DATASET.$tbl"
done

section "IAM"
# `disabled` is omitted from the output when the account is enabled, so only "True" means disabled.
for sa in "$DATAFLOW_SA" "$WORKFLOW_SA"; do
  check "service account $sa exists and enabled" bash -c \
    'out=$(gcloud iam service-accounts describe "$1" --project "$2" --format="value(disabled)") && [[ "$out" != "True" ]]' \
    _ "$sa" "$PROJECT_ID"
done

section "Artifact Registry / Dataflow template"
check "repository $ARTIFACT_REPO" gcloud artifacts repositories describe "$ARTIFACT_REPO" --location "$REGION" --project "$PROJECT_ID"
warn_check "image $IMAGE_URI (make docker)" gcloud artifacts docker images describe "$IMAGE_URI" --project "$PROJECT_ID"
warn_check "template spec $TEMPLATE_PATH (make docker)" gcloud storage objects describe "$TEMPLATE_PATH" --project "$PROJECT_ID"

printf "\n%sSummary:%s %s%d ok%s, %s%d failed%s, %s%d warnings%s\n" \
  "$BOLD" "$RESET" "$GREEN" "$PASSED" "$RESET" "$RED" "$FAILED" "$RESET" "$YELLOW" "$WARNED" "$RESET"
[[ $FAILED -eq 0 ]]
