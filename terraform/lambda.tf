data "archive_file" "extraction_lambda" {
    type        = "zip"
    source_file = "${path.module}/../src/extraction_lambda/${var.extraction_lambda}.py"
    output_path = "${path.module}/../deployments/${var.extraction_lambda}.zip"
}

## need to add dependencies to the zip files. - maybe layers pip install 

resource "aws_lambda_function" "extraction_lambda" {
  function_name = "${var.extraction_lambda}"
  s3_bucket     = "${aws_s3_bucket.lambda_bucket.id}"
  s3_key        = "${aws_s3_object.extaction_file_upload.key}"
  role          = aws_iam_role.iam_role_extraction_lambda.arn
  handler       = "${var.extraction_lambda}.lambda_handler"
  source_code_hash = data.archive_file.extraction_lambda.output_base64sha256
  runtime = "python3.13"
}

