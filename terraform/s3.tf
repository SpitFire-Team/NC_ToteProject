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


# resource "aws_s3_bucket" "lambda_bucket" {
#   bucket_prefix = "lambda-bucket-"

#   tags = {
#     Name        = "Lambda storage bucket"
#     Environment = "Dev"
#   }
# }

# resource "aws_s3_object" "extaction_file_upload" {
#   bucket = "${aws_s3_bucket.lambda_bucket.id}"
#   key = "lambda-functions/${var.extraction_lambda}.zip"
#   source = "${data.archive_file.extraction_lambda.output_path}"
# }