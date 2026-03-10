output "landing_bucket_name" {
  value = google_storage_bucket.landing.name
}

output "pipeline_service_account_email" {
  value = google_service_account.pipeline.email
}

output "bronze_dataset_id" {
  value = google_bigquery_dataset.bronze.dataset_id
}

output "silver_dataset_id" {
  value = google_bigquery_dataset.silver.dataset_id
}

output "gold_dataset_id" {
  value = google_bigquery_dataset.gold.dataset_id
}

output "metadata_dataset_id" {
  value = google_bigquery_dataset.metadata.dataset_id
}
