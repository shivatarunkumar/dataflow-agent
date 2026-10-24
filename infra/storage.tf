locals {
  bucket_prefix = "${var.project_id}-${var.prefix}"
}

# Files land under landing/, and are moved to archive/ or invalid/ after processing.
# manual/ is used by `make run-dataflow` (no notification on that prefix).
resource "google_storage_bucket" "landing" {
  name                        = "${local.bucket_prefix}-landing"
  location                    = var.region
  uniform_bucket_level_access = true
  force_destroy               = true

  depends_on = [google_project_service.services]
}

# Dataflow temp/staging files and the flex template spec.
resource "google_storage_bucket" "dataflow" {
  name                        = "${local.bucket_prefix}-dataflow"
  location                    = var.region
  uniform_bucket_level_access = true
  force_destroy               = true

  lifecycle_rule {
    condition {
      age            = 7
      matches_prefix = ["temp/"]
    }
    action {
      type = "Delete"
    }
  }

  # Delivery claims (workflows/ingest_file.yaml) only need to outlive Pub/Sub's 7-day redelivery window.
  lifecycle_rule {
    condition {
      age            = 8
      matches_prefix = ["claims/"]
    }
    action {
      type = "Delete"
    }
  }

  depends_on = [google_project_service.services]
}
