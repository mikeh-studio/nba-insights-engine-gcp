resource "google_storage_bucket" "landing" {
  name                        = var.gcs_bucket_name
  location                    = var.bucket_location
  uniform_bucket_level_access = true
}

resource "google_bigquery_dataset" "bronze" {
  dataset_id = var.bronze_dataset
  location   = var.bucket_location
}

resource "google_bigquery_dataset" "silver" {
  dataset_id = var.silver_dataset
  location   = var.bucket_location
}

resource "google_bigquery_dataset" "gold" {
  dataset_id = var.gold_dataset
  location   = var.bucket_location
}

resource "google_bigquery_dataset" "metadata" {
  dataset_id = var.metadata_dataset
  location   = var.bucket_location
}

resource "google_service_account" "pipeline" {
  account_id   = var.service_account_id
  display_name = "NBA pipeline runtime"
}

resource "google_project_iam_member" "bq_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.pipeline.email}"
}

resource "google_project_iam_member" "bq_data_editor" {
  project = var.project_id
  role    = "roles/bigquery.dataEditor"
  member  = "serviceAccount:${google_service_account.pipeline.email}"
}

resource "google_project_iam_member" "storage_admin" {
  project = var.project_id
  role    = "roles/storage.objectAdmin"
  member  = "serviceAccount:${google_service_account.pipeline.email}"
}
