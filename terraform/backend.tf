/**
 * TERRAFORM STATE INFRASTRUCTURE
 * Resources that manage the S3 bucket where our .tfstate files are stored.
 */

# The S3 Bucket for state storage
resource "aws_s3_bucket" "terraform_state" {
  bucket = "${var.project_name}-tfstate-${var.environment}"

  tags = merge(
    local.common_tags,
    { Name = "Terraform Remote State Storage" }
  )
}

# Versioning
resource "aws_s3_bucket_versioning" "terraform_state_versioning" {
  bucket = aws_s3_bucket.terraform_state.id
  versioning_configuration {
    status = "Enabled"
  }
}

# Prevent public access to sensitive state data (contains secrets/passwords)
resource "aws_s3_bucket_public_access_block" "terraform_state_lock" {
  bucket = aws_s3_bucket.terraform_state.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}