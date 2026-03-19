output "redshift_endpoint" {
  value = aws_redshiftserverless_workgroup.main.endpoint
}

output "redshift_workgroup_name" {
  value = aws_redshiftserverless_workgroup.main.workgroup_name
}

output "s3_bucket_name" {
  value = aws_s3_bucket.landing.bucket
}

output "s3_bucket_arn" {
  value = aws_s3_bucket.landing.arn
}

output "redshift_iam_role_arn" {
  value = aws_iam_role.redshift_s3.arn
}
