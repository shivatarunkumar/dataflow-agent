resource "google_service_account" "dataflow" {
  account_id   = "${var.prefix}-dataflow"
  display_name = "Dataflow worker for GCS to BQ ingestion"
}

# Identity for the Eventarc trigger and the workflow it starts.
resource "google_service_account" "workflow" {
  account_id   = "${var.prefix}-workflow"
  display_name = "Workflow orchestrating GCS to BQ ingestion"
}

locals {
  dataflow_roles = [
    "roles/dataflow.worker",
    "roles/bigquery.dataEditor",
    "roles/bigquery.jobUser",
    "roles/artifactregistry.reader",
  ]
  workflow_roles = [
    "roles/dataflow.developer",     # launch + poll flex template jobs
    "roles/bigquery.dataEditor",    # job_metadata DML
    "roles/bigquery.jobUser",       # run queries
    "roles/logging.logWriter",      # workflow execution logs
    "roles/workflows.invoker",      # Eventarc -> workflow
    "roles/eventarc.eventReceiver", # receive events on the trigger
  ]
}

resource "google_project_iam_member" "dataflow" {
  for_each = toset(local.dataflow_roles)

  project = var.project_id
  role    = each.value
  member  = "serviceAccount:${google_service_account.dataflow.email}"
}

resource "google_project_iam_member" "workflow" {
  for_each = toset(local.workflow_roles)

  project = var.project_id
  role    = each.value
  member  = "serviceAccount:${google_service_account.workflow.email}"
}

# Both identities read/write the landing and dataflow buckets.
resource "google_storage_bucket_iam_member" "buckets" {
  for_each = {
    for pair in setproduct(
      ["landing", "dataflow"],
      [google_service_account.dataflow.email, google_service_account.workflow.email],
    ) : "${pair[0]}-${pair[1]}" => { bucket = pair[0], email = pair[1] }
  }

  bucket = each.value.bucket == "landing" ? google_storage_bucket.landing.name : google_storage_bucket.dataflow.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${each.value.email}"
}

# The workflow launches Dataflow jobs that run as the dataflow service account.
resource "google_service_account_iam_member" "workflow_acts_as_dataflow" {
  service_account_id = google_service_account.dataflow.name
  role               = "roles/iam.serviceAccountUser"
  member             = "serviceAccount:${google_service_account.workflow.email}"
}

# Pub/Sub's service agent mints OIDC tokens for Eventarc's authenticated push.
resource "google_project_iam_member" "pubsub_token_creator" {
  project = var.project_id
  role    = "roles/iam.serviceAccountTokenCreator"
  member  = "serviceAccount:service-${data.google_project.this.number}@gcp-sa-pubsub.iam.gserviceaccount.com"

  depends_on = [google_project_service.services]
}
