resource "google_artifact_registry_repository" "dataflow" {
  repository_id = "${var.prefix}-dataflow"
  location      = var.region
  format        = "DOCKER"
  description   = "Dataflow flex template images."

  depends_on = [google_project_service.services]
}

locals {
  template_path = "gs://${google_storage_bucket.dataflow.name}/templates/gcs_to_bq.json"
  image_uri     = "${var.region}-docker.pkg.dev/${var.project_id}/${google_artifact_registry_repository.dataflow.repository_id}/gcs-to-bq:latest"
}
