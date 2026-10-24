resource "google_pubsub_topic" "file_landed" {
  name       = "${var.prefix}-file-landed"
  depends_on = [google_project_service.services]
}

# The GCS service agent publishes bucket notifications.
data "google_storage_project_service_account" "gcs" {
  depends_on = [google_project_service.services]
}

resource "google_pubsub_topic_iam_member" "gcs_publisher" {
  topic  = google_pubsub_topic.file_landed.id
  role   = "roles/pubsub.publisher"
  member = "serviceAccount:${data.google_storage_project_service_account.gcs.email_address}"
}

resource "google_storage_notification" "landing" {
  bucket             = google_storage_bucket.landing.name
  topic              = google_pubsub_topic.file_landed.id
  payload_format     = "JSON_API_V1"
  event_types        = ["OBJECT_FINALIZE"]
  object_name_prefix = "landing/"

  depends_on = [google_pubsub_topic_iam_member.gcs_publisher]
}

# The subscription is created and owned by the Eventarc trigger (workflows.tf).

# Error events for failed/rejected jobs (published by the workflow; see project.md 7a).
resource "google_pubsub_topic" "ingest_errors" {
  name                       = "${var.prefix}-ingest-errors"
  message_retention_duration = "604800s"
  depends_on                 = [google_project_service.services]
}

# Holds events until a consumer (e.g. an investigation agent) exists; a topic alone drops messages.
resource "google_pubsub_subscription" "ingest_errors" {
  name                       = "${var.prefix}-ingest-errors-sub"
  topic                      = google_pubsub_topic.ingest_errors.id
  ack_deadline_seconds       = 60
  message_retention_duration = "604800s"
  expiration_policy {
    ttl = "" # never expire for inactivity
  }
}

resource "google_pubsub_topic_iam_member" "workflow_error_publisher" {
  topic  = google_pubsub_topic.ingest_errors.id
  role   = "roles/pubsub.publisher"
  member = "serviceAccount:${google_service_account.workflow.email}"
}
