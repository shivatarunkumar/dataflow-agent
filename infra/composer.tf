# Cloud Composer 3 (Airflow) environment. It costs ~$0.80-1.00/hour even when idle, so it only exists while
# enable_composer = true: `make airflow-up` creates it, `make airflow-down` deletes it (nothing else is touched).
# File events wait on the dfa-file-landed-airflow subscription (7 days) while it is down.

locals {
  composer_name = "${var.prefix}-composer"
}

resource "google_composer_environment" "airflow" {
  count = var.enable_composer ? 1 : 0

  name   = local.composer_name
  region = var.region

  config {
    environment_size = "ENVIRONMENT_SIZE_SMALL"

    software_config {
      image_version = var.composer_image

      # Read by dags/dfa_common/config.py.
      env_variables = {
        DFA_PROJECT_ID          = var.project_id
        DFA_REGION              = var.region
        DFA_LANDING_BUCKET      = google_storage_bucket.landing.name
        DFA_DATAFLOW_BUCKET     = google_storage_bucket.dataflow.name
        DFA_SUBSCRIPTION        = google_pubsub_subscription.file_landed_airflow.name
        DFA_ERROR_TOPIC         = google_pubsub_topic.ingest_errors.name
        DFA_TEMPLATE_PATH       = local.template_path
        DFA_AUDIT_DATASET       = var.audit_dataset
        DFA_TARGET_DATASET      = var.target_dataset
        DFA_DATAFLOW_SA         = google_service_account.dataflow.email
        DFA_MAX_CONCURRENT_JOBS = tostring(var.max_concurrent_jobs)
        DFA_COMPOSER_ENV        = local.composer_name
      }

      airflow_config_overrides = {
        # New DAGs start unpaused, so a fresh environment processes queued files straight away.
        "core-dags_are_paused_at_creation" = "False"
        # Principals that sign in (incl. the df-agent SA) get read-only Airflow access; see `make airflow-admin`.
        "webserver-rbac_user_registration_role" = "Viewer"
      }
    }

    # Smallest workable sizes: the DAGs only call APIs; the heavy lifting runs in Dataflow.
    workloads_config {
      scheduler {
        cpu        = 0.5
        memory_gb  = 2
        storage_gb = 1
        count      = 1
      }
      web_server {
        cpu        = 0.5
        memory_gb  = 2
        storage_gb = 1
      }
      worker {
        cpu        = 0.5
        memory_gb  = 2
        storage_gb = 1
        min_count  = 1
        max_count  = 2
      }
      dag_processor {
        cpu        = 0.5
        memory_gb  = 2
        storage_gb = 1
        count      = 1
      }
    }

    node_config {
      service_account = google_service_account.composer.email
    }
  }

  depends_on = [
    google_project_iam_member.composer,
    google_service_account_iam_member.composer_service_agent,
    google_service_account_iam_member.composer_acts_as_dataflow,
    google_pubsub_subscription_iam_member.composer_file_landed_subscriber,
    google_pubsub_topic_iam_member.composer_error_publisher,
  ]
}
