resource "google_bigquery_dataset" "audit" {
  dataset_id                 = var.audit_dataset
  location                   = var.region
  delete_contents_on_destroy = true

  depends_on = [google_project_service.services]
}

resource "google_bigquery_dataset" "target" {
  dataset_id                 = var.target_dataset
  location                   = var.region
  delete_contents_on_destroy = true

  depends_on = [google_project_service.services]
}

resource "google_bigquery_table" "audit" {
  for_each = toset(["job_metadata", "validation_metadata"])

  dataset_id          = google_bigquery_dataset.audit.dataset_id
  table_id            = each.value
  schema              = file("${path.module}/bq/${each.value}.json")
  deletion_protection = false

  time_partitioning {
    type  = "DAY"
    field = "created_at"
  }
}

resource "google_bigquery_table" "target" {
  for_each = toset(var.target_tables)

  dataset_id          = google_bigquery_dataset.target.dataset_id
  table_id            = each.value
  schema              = file("${path.module}/bq/${each.value}.json")
  deletion_protection = false

  time_partitioning {
    type  = "DAY"
    field = "_loaded_at"
  }
}
