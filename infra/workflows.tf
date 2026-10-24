locals {
  template_path = "gs://${google_storage_bucket.dataflow.name}/templates/gcs_to_bq.json"
  image_uri     = "${var.region}-docker.pkg.dev/${var.project_id}/${google_artifact_registry_repository.dataflow.repository_id}/gcs-to-bq:latest"
}

resource "google_workflows_workflow" "ingest" {
  name                = "${var.prefix}-ingest-file"
  region              = var.region
  description         = "Per-file ingestion: register job, run Dataflow flex template, finalize audit and move file."
  service_account     = google_service_account.workflow.id
  source_contents     = file("${path.module}/../workflows/ingest_file.yaml")
  call_log_level      = "LOG_ERRORS_ONLY"
  deletion_protection = false

  # Read in the workflow with sys.get_env().
  user_env_vars = {
    DFA_REGION              = var.region
    DFA_AUDIT_DATASET       = var.audit_dataset
    DFA_TARGET_DATASET      = var.target_dataset
    DFA_TEMPLATE_PATH       = local.template_path
    DFA_DATAFLOW_SA         = google_service_account.dataflow.email
    DFA_DATAFLOW_BUCKET     = google_storage_bucket.dataflow.name
    DFA_MAX_CONCURRENT_JOBS = tostring(var.max_concurrent_jobs)
    DFA_ERROR_TOPIC         = google_pubsub_topic.ingest_errors.name
  }

  depends_on = [
    google_project_iam_member.workflow,
    google_pubsub_topic_iam_member.workflow_error_publisher,
  ]
}

# One workflow execution per message on the GCS notification topic.
resource "google_eventarc_trigger" "file_landed" {
  name            = "${var.prefix}-file-landed"
  location        = var.region
  service_account = google_service_account.workflow.email

  matching_criteria {
    attribute = "type"
    value     = "google.cloud.pubsub.topic.v1.messagePublished"
  }

  transport {
    pubsub {
      topic = google_pubsub_topic.file_landed.id
    }
  }

  destination {
    workflow = google_workflows_workflow.ingest.id
  }

  depends_on = [
    google_project_iam_member.workflow,
    google_project_iam_member.pubsub_token_creator,
  ]
}
