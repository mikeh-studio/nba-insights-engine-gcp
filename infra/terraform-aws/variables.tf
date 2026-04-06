variable "aws_region" {
  type    = string
  default = "us-east-1"
}

variable "namespace_name" {
  type    = string
  default = "nba-analytics"
}

variable "workgroup_name" {
  type    = string
  default = "nba-analytics-wg"
}

variable "redshift_db_name" {
  type    = string
  default = "nba_analytics"
}

variable "redshift_admin_username" {
  type    = string
  default = "admin"
}

variable "redshift_admin_password" {
  type      = string
  sensitive = true
}

variable "redshift_base_capacity" {
  description = "Redshift Serverless base RPU capacity"
  type        = number
  default     = 8
}

variable "s3_bucket_name" {
  type = string
}

variable "vpc_id" {
  description = "VPC ID for Redshift networking (no default VPC assumption)"
  type        = string
}

variable "subnet_ids" {
  description = "Subnet IDs for Redshift workgroup placement"
  type        = list(string)
}

variable "allowed_cidr_blocks" {
  description = "CIDR blocks allowed to connect to Redshift (port 5439)"
  type        = list(string)
  default     = ["10.0.0.0/8"]
}

variable "tags" {
  type = map(string)
  default = {
    project = "nba-analytics"
    managed = "terraform"
  }
}
