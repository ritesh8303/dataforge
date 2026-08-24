resource "aws_s3_bucket" "this" {
  bucket        = var.bucket_name
  force_destroy = var.force_destroy
}

resource "aws_s3_bucket_versioning" "this" {
  bucket = aws_s3_bucket.this.id
  versioning_configuration {
    status = "Suspended"
  }
}

# Optional expiry: only raw layers should auto-delete. Buckets holding SCD
# history must NOT expire objects, or the lifecycle silently erases history.
resource "aws_s3_bucket_lifecycle_configuration" "this" {
  count  = var.expiration_days == null ? 0 : 1
  bucket = aws_s3_bucket.this.id
  rule {
    id     = "expire-old-objects"
    status = "Enabled"
    filter {}
    expiration {
      days = var.expiration_days
    }
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "this" {
  bucket = aws_s3_bucket.this.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# This makes the bucket private (Good practice)
resource "aws_s3_bucket_public_access_block" "this" {
  bucket                  = aws_s3_bucket.this.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}
variable "bucket_name" {
  description = "The name of the bucket"
  type        = string
}

variable "force_destroy" {
  description = "Allow terraform destroy to delete a non-empty bucket. Keep false for data buckets."
  type        = bool
  default     = false
}

variable "expiration_days" {
  description = "Days after which objects expire. null disables the lifecycle rule entirely."
  type        = number
  default     = null
}