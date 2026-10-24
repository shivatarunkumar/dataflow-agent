# df-agent (incident-response agent): least-privilege identity + its append-only incidents table.
# The SA can read everything it needs to investigate, create (never delete/overwrite) objects in the landing
# bucket for retries, and write only to agent_incidents. See df-agent/df-agent.md §7.

resource "google_service_account" "agent" {
  account_id   = "${var.prefix}-agent"
  display_name = "df-agent incident-response agent (read-only + create-only retries)"
}

resource "google_bigquery_table" "agent_incidents" {
  dataset_id          = google_bigquery_dataset.audit.dataset_id
  table_id            = "agent_incidents"
  schema              = file("${path.module}/bq/agent_incidents.json")
  deletion_protection = false

  time_partitioning {
    type  = "DAY"
    field = "created_at"
  }
}

locals {
  agent_project_roles = [
    "roles/logging.viewer",   # Cloud Logging
    "roles/dataflow.viewer",  # job state + messages
    "roles/workflows.viewer", # workflow executions
    "roles/bigquery.jobUser", # run read queries
    "roles/compute.viewer",   # regional quota usage
    # Hypothesis verification (read-only): IAM policies + role definitions, and dataset metadata/ACLs (no table data).
    "roles/iam.securityReviewer",
    "roles/bigquery.metadataViewer",
  ]
}

resource "google_project_iam_member" "agent" {
  for_each = toset(local.agent_project_roles)

  project = var.project_id
  role    = each.value
  member  = "serviceAccount:${google_service_account.agent.email}"
}

# Read the audit tables...
resource "google_bigquery_dataset_iam_member" "agent_audit_reader" {
  dataset_id = google_bigquery_dataset.audit.dataset_id
  role       = "roles/bigquery.dataViewer"
  member     = "serviceAccount:${google_service_account.agent.email}"
}

# ...and write only its own table.
resource "google_bigquery_table_iam_member" "agent_incidents_writer" {
  dataset_id = google_bigquery_dataset.audit.dataset_id
  table_id   = google_bigquery_table.agent_incidents.table_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.agent.email}"
}

# Landing bucket: read + create-only (objectCreator cannot delete or overwrite existing objects).
resource "google_storage_bucket_iam_member" "agent_landing" {
  for_each = toset(["roles/storage.objectViewer", "roles/storage.objectCreator"])

  bucket = google_storage_bucket.landing.name
  role   = each.value
  member = "serviceAccount:${google_service_account.agent.email}"
}

resource "google_storage_bucket_iam_member" "agent_dataflow_bucket_reader" {
  bucket = google_storage_bucket.dataflow.name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_service_account.agent.email}"
}

resource "google_pubsub_subscription_iam_member" "agent_error_subscriber" {
  subscription = google_pubsub_subscription.ingest_errors.id
  role         = "roles/pubsub.subscriber"
  member       = "serviceAccount:${google_service_account.agent.email}"
}

# Lets the operator (you) run the agent locally as dfa-agent via impersonation.
resource "google_service_account_iam_member" "agent_operator" {
  count = var.agent_operator == "" ? 0 : 1

  service_account_id = google_service_account.agent.name
  role               = "roles/iam.serviceAccountTokenCreator"
  member             = var.agent_operator
}
