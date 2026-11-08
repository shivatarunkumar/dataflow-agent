resource "google_service_account" "dataflow" {
  account_id   = "${var.prefix}-dataflow"
  display_name = "Dataflow worker for GCS to BQ ingestion"
}

# Identity of the Composer environment (Airflow scheduler/workers run the DAGs as this SA).
# It always exists (it's free), so its IAM survives `make airflow-down` / `airflow-up` cycles.
resource "google_service_account" "composer" {
  account_id   = "${var.prefix}-composer"
  display_name = "Composer (Airflow) orchestrating GCS to BQ ingestion"
}

locals {
  dataflow_roles = [
    "roles/dataflow.worker",
    "roles/bigquery.dataEditor",
    "roles/bigquery.jobUser",
    "roles/artifactregistry.reader",
  ]
  composer_roles = [
    "roles/composer.worker",     # required for a Composer environment SA
    "roles/dataflow.developer",  # launch + watch flex template jobs, read job messages
    "roles/bigquery.dataEditor", # job_metadata DML
    "roles/bigquery.jobUser",    # run queries
    "roles/logging.logWriter",   # Airflow task logs
  ]
}

resource "google_project_iam_member" "dataflow" {
  for_each = toset(local.dataflow_roles)

  project = var.project_id
  role    = each.value
  member  = "serviceAccount:${google_service_account.dataflow.email}"
}

resource "google_project_iam_member" "composer" {
  for_each = toset(local.composer_roles)

  project = var.project_id
  role    = each.value
  member  = "serviceAccount:${google_service_account.composer.email}"
}

# Both identities read/write the landing and dataflow buckets.
resource "google_storage_bucket_iam_member" "buckets" {
  for_each = {
    for pair in setproduct(
      ["landing", "dataflow"],
      [google_service_account.dataflow.email, google_service_account.composer.email],
    ) : "${pair[0]}-${pair[1]}" => { bucket = pair[0], email = pair[1] }
  }

  bucket = each.value.bucket == "landing" ? google_storage_bucket.landing.name : google_storage_bucket.dataflow.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${each.value.email}"
}

# Airflow launches Dataflow jobs that run as the dataflow service account.
resource "google_service_account_iam_member" "composer_acts_as_dataflow" {
  service_account_id = google_service_account.dataflow.name
  role               = "roles/iam.serviceAccountUser"
  member             = "serviceAccount:${google_service_account.composer.email}"
}

# Composer 3: the Composer service agent must be able to manage the environment's SA.
resource "google_service_account_iam_member" "composer_service_agent" {
  service_account_id = google_service_account.composer.name
  role               = "roles/composer.ServiceAgentV2Ext"
  member             = "serviceAccount:service-${data.google_project.this.number}@cloudcomposer-accounts.iam.gserviceaccount.com"

  depends_on = [google_project_service.services]
}
