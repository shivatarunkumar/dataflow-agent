output "project_id" {
  value = var.project_id
}

output "region" {
  value = var.region
}

output "landing_bucket" {
  value = google_storage_bucket.landing.name
}

output "dataflow_bucket" {
  value = google_storage_bucket.dataflow.name
}

output "topic" {
  value = google_pubsub_topic.file_landed.name
}

output "error_topic" {
  value = google_pubsub_topic.ingest_errors.name
}

output "error_subscription" {
  value = google_pubsub_subscription.ingest_errors.name
}

output "agent_sa" {
  value = google_service_account.agent.email
}

output "template_path" {
  value = local.template_path
}

output "image_uri" {
  value = local.image_uri
}

output "artifact_repo" {
  value = google_artifact_registry_repository.dataflow.repository_id
}

output "dataflow_sa" {
  value = google_service_account.dataflow.email
}

output "composer_sa" {
  value = google_service_account.composer.email
}

output "file_landed_subscription" {
  value = google_pubsub_subscription.file_landed_airflow.name
}

# Composer outputs are empty while enable_composer = false.
output "composer_env" {
  value = var.enable_composer ? local.composer_name : ""
}

output "dag_gcs_prefix" {
  value = try(google_composer_environment.airflow[0].config[0].dag_gcs_prefix, "")
}

output "airflow_uri" {
  value = try(google_composer_environment.airflow[0].config[0].airflow_uri, "")
}

output "audit_dataset" {
  value = var.audit_dataset
}

output "target_dataset" {
  value = var.target_dataset
}

output "target_tables" {
  value = join(",", var.target_tables)
}

output "services" {
  value = join(",", local.services)
}
