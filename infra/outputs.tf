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

output "workflow_sa" {
  value = google_service_account.workflow.email
}

output "workflow_name" {
  value = google_workflows_workflow.ingest.name
}

output "eventarc_trigger" {
  value = google_eventarc_trigger.file_landed.name
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
