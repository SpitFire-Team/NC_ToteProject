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


resource "aws_s3_bucket" "code_bucket" {
  bucket_prefix = "code-bucket-"

  tags = {
    Name        = "Code storage bucket"
    Environment = "Dev"
  }
}

resource "aws_s3_object" "lambda_layer" {
  bucket = aws_s3_bucket.code_bucket.bucket
  key    = "layer/layer.zip"
  source = data.archive_file.layer_code.output_path
  etag   = filemd5(data.archive_file.layer_code.output_path)
  depends_on = [ data.archive_file.layer_code ]
}



