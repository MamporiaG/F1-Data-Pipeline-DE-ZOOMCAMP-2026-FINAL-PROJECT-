output "gcs_bucket_name" {
  description = "GCS bucket name — copy this into your .env as GCS_BUCKET_NAME"
  value       = google_storage_bucket.f1_data_lake.name
}

output "gcs_bucket_url" {
  value = google_storage_bucket.f1_data_lake.url
}

output "bq_raw_dataset" {
  value = google_bigquery_dataset.f1_raw.dataset_id
}

output "bq_staging_dataset" {
  value = google_bigquery_dataset.f1_staging.dataset_id
}

output "bq_marts_dataset" {
  value = google_bigquery_dataset.f1_marts.dataset_id
}
