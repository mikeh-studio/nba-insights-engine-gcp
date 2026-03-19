# --- S3 landing bucket for cross-cloud data movement ---

resource "aws_s3_bucket" "landing" {
  bucket = var.s3_bucket_name
  tags   = var.tags
}

resource "aws_s3_bucket_server_side_encryption_configuration" "landing" {
  bucket = aws_s3_bucket.landing.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "landing" {
  bucket                  = aws_s3_bucket.landing.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "landing" {
  bucket = aws_s3_bucket.landing.id

  rule {
    id     = "expire-landing-files"
    status = "Enabled"

    expiration {
      days = 30
    }

    filter {
      prefix = "nba_data/"
    }
  }
}

# --- IAM role for Redshift to read from S3 ---

data "aws_iam_policy_document" "redshift_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["redshift.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "redshift_s3" {
  name               = "${var.namespace_name}-redshift-s3"
  assume_role_policy = data.aws_iam_policy_document.redshift_assume.json
  tags               = var.tags
}

data "aws_iam_policy_document" "redshift_s3_access" {
  statement {
    actions = [
      "s3:GetObject",
      "s3:GetBucketLocation",
      "s3:ListBucket",
    ]
    resources = [
      aws_s3_bucket.landing.arn,
      "${aws_s3_bucket.landing.arn}/*",
    ]
  }
}

resource "aws_iam_role_policy" "redshift_s3" {
  name   = "${var.namespace_name}-redshift-s3-read"
  role   = aws_iam_role.redshift_s3.id
  policy = data.aws_iam_policy_document.redshift_s3_access.json
}

# --- Redshift Serverless ---

resource "aws_redshiftserverless_namespace" "main" {
  namespace_name      = var.namespace_name
  db_name             = var.redshift_db_name
  admin_username      = var.redshift_admin_username
  admin_user_password = var.redshift_admin_password
  iam_roles           = [aws_iam_role.redshift_s3.arn]
  tags                = var.tags
}

resource "aws_redshiftserverless_workgroup" "main" {
  namespace_name = aws_redshiftserverless_namespace.main.namespace_name
  workgroup_name = var.workgroup_name
  base_capacity  = var.redshift_base_capacity
  publicly_accessible = false
  tags           = var.tags
}
