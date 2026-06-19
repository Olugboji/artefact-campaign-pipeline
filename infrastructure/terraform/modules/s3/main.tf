variable "env" {}

resource "aws_s3_bucket" "raw" {
  bucket        = "artefact-${var.env}-raw"
  force_destroy = var.env != "prod"
}

resource "aws_s3_bucket_versioning" "raw" {
  bucket = aws_s3_bucket.raw.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket" "staging" {
  bucket        = "artefact-${var.env}-staging"
  force_destroy = var.env != "prod"
}

output "raw_bucket_name" {
  value = aws_s3_bucket.raw.bucket
}

output "staging_bucket_name" {
  value = aws_s3_bucket.staging.bucket
}