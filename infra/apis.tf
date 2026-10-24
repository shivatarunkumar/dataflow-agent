locals {
  services = [
    "artifactregistry.googleapis.com",
    "bigquery.googleapis.com",
    "cloudbuild.googleapis.com",
    "cloudresourcemanager.googleapis.com",
    "compute.googleapis.com",
    "dataflow.googleapis.com",
    "eventarc.googleapis.com",
    "iam.googleapis.com",
    "pubsub.googleapis.com",
    "storage.googleapis.com",
    "workflowexecutions.googleapis.com",
    "workflows.googleapis.com",
  ]
}

resource "google_project_service" "services" {
  for_each = toset(local.services)

  service = each.value
  # Leave APIs enabled on destroy: disabling them can break other workloads in the project.
  disable_on_destroy = false
}
