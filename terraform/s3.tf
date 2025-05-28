resource "aws_s3_bucket" "nc-totes-ingestion-bucket" {
  bucket = "nc-totes-ingestion-bucket-swraw"

  tags = {
    Name        = "Ingestion bucket"
    Environment = "Dev"
  }
}