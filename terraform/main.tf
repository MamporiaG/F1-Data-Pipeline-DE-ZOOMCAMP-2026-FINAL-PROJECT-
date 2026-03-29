
terraform {
  required_version = ">= 1.3"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  project     = var.project_id
  region      = var.region
  credentials = file(var.credentials_file)
}

resource "google_storage_bucket" "f1_data_lake" {
  name                        = "${var.project_id}-f1-data-lake"
  location                    = var.region
  storage_class               = "STANDARD"
  uniform_bucket_level_access = true

  versioning {
    enabled = true
  }

  lifecycle_rule {
    condition {
      age = 90
    }
    action {
      type = "Delete"
    }
  }

  force_destroy = true  # allows `terraform destroy` even with files in it
}

resource "google_bigquery_dataset" "f1_raw" {
  dataset_id                  = "f1_raw"
  friendly_name               = "F1 Raw"
  description                 = "Raw F1 data loaded directly from GCS. Never modified."
  location                    = var.region
  delete_contents_on_destroy  = true
}


resource "google_bigquery_dataset" "f1_staging" {
  dataset_id                  = "f1_staging"
  friendly_name               = "F1 Staging"
  description                 = "dbt staging layer — cleaned and typed versions of raw tables."
  location                    = var.region
  delete_contents_on_destroy  = true
}


resource "google_bigquery_dataset" "f1_marts" {
  dataset_id                  = "f1_marts"
  friendly_name               = "F1 Marts"
  description                 = "dbt mart layer — facts and dimensions for Looker Studio."
  location                    = var.region
  delete_contents_on_destroy  = true
}
