resource "google_artifact_registry_repository" "dataflow" {
  repository_id = "${var.prefix}-dataflow"
  location      = var.region
  format        = "DOCKER"
  description   = "Dataflow flex template images."

  depends_on = [google_project_service.services]
}
