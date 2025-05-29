resource "aws_s3_bucket" "ingestion_bucket" {
  bucket_prefix = "ingested-data-bucket-"

  tags = {
    Name        = "Ingested data bucket"
    Environment = "Dev"
  }
}

resource "aws_s3_bucket" "processed_bucket" {
  bucket_prefix = "processed-data-bucket-"

  tags = {
    Name        = "Processed data bucket"
    Environment = "Dev"
  }
}