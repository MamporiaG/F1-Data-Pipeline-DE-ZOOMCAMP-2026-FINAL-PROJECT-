

variable "project_id" {
  description = "Your GCP Project ID (visible in GCP Console top bar)"
  type        = string
}

variable "region" {
  description = "GCP region. US multi-region is cheapest and has best BigQuery availability."
  type        = string
  default     = "US"
}

variable "credentials_file" {
  description = "Path to your GCP service account JSON key file"
  type        = string
  default     = "../credentials/gcp_credentials.json"
}
