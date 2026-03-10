variable "project_id" {
  type = string
}

variable "region" {
  type    = string
  default = "us-central1"
}

variable "gcs_bucket_name" {
  type = string
}

variable "bucket_location" {
  type    = string
  default = "US"
}

variable "bronze_dataset" {
  type    = string
  default = "nba_bronze"
}

variable "silver_dataset" {
  type    = string
  default = "nba_silver"
}

variable "gold_dataset" {
  type    = string
  default = "nba_gold"
}

variable "metadata_dataset" {
  type    = string
  default = "nba_metadata"
}

variable "service_account_id" {
  type    = string
  default = "nba-pipeline-runner"
}
