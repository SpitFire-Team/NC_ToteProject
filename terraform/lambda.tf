

data "archive_file" "extraction_lambda" {
    type        = "zip"
    source_file = "${path.module}/../src/extraction_lambda/${var.extraction_lambda}.py"
    output_path = "${path.module}/../deployments/${var.extraction_lambda}.zip"
}

resource "aws_s3_object" "extaction_file_upload" {
  bucket = "${aws_s3_bucket.code_bucket.id}"
  key = "lambda-functions/${var.extraction_lambda}.zip"
  source = "${data.archive_file.extraction_lambda.output_path}"
}

## need to add dependencies to the zip files. - maybe layers pip install 

resource "aws_lambda_function" "extraction_lambda" {
  function_name = "${var.extraction_lambda}"
  s3_bucket     = "${aws_s3_bucket.code_bucket.id}"
  s3_key        = "${aws_s3_object.extaction_file_upload.key}"
  role          = aws_iam_role.iam_role_extraction_lambda.arn
  handler       = "${var.extraction_lambda}.lambda_handler"
  source_code_hash = data.archive_file.extraction_lambda.output_base64sha256
  layers = [aws_lambda_layer_version.layer.arn]
  runtime = var.runtime
}
 ## Terraform dummy for Lambda step_functions

  ## Dummy extraction lambda
data "archive_file" "dummy_extraction_lambda" {
    type        = "zip"
    source_file = "${path.module}/../src/dummy_lambda/dummy_${var.extraction_lambda}.py"
    output_path = "${path.module}/../deployments/dummy_lambda/dummy_lambda_${var.extraction_lambda}.zip"
}

resource "aws_s3_object" "dummy_extaction_file_upload" {
  bucket = "${aws_s3_bucket.code_bucket.id}"
  key = "lambda-functions/dummy_lambda/dummy_lambda_${var.extraction_lambda}.zip"
  source = "${data.archive_file.dummy_extraction_lambda.output_path}"
}


resource "aws_lambda_function" "dummy_extraction_lambda" {
  function_name = "dummy_${var.extraction_lambda}"
  s3_bucket     = "${aws_s3_bucket.code_bucket.id}"
  s3_key        = "${aws_s3_object.dummy_extaction_file_upload.key}"
  role          = aws_iam_role.iam_role_extraction_lambda.arn
  handler       = "dummy_${var.extraction_lambda}.lambda_handler"
  source_code_hash = data.archive_file.dummy_extraction_lambda.output_base64sha256
  runtime = var.runtime
}


  # Dummy transform lambda

data "archive_file" "dummy_transform_lambda" {
    type        = "zip"
    source_file = "${path.module}/../src/dummy_lambda/dummy_${var.transform_lambda}.py"
    output_path = "${path.module}/../deployments/dummy_lambda/dummy_lambda_${var.transform_lambda}.zip"
}

resource "aws_s3_object" "dummy_transform_file_upload" {
  bucket = "${aws_s3_bucket.code_bucket.id}"
  key = "lambda-functions/dummy_lambda/dummy_lambda_${var.transform_lambda}.zip"
  source = "${data.archive_file.dummy_transform_lambda.output_path}"
}

resource "aws_lambda_function" "dummy_transform_lambda" {
  function_name = "dummy_${var.transform_lambda}"
  s3_bucket     = "${aws_s3_bucket.code_bucket.id}"
  s3_key        = "${aws_s3_object.dummy_transform_file_upload.key}"
  role          = aws_iam_role.iam_role_extraction_lambda.arn
  handler       = "dummy_${var.transform_lambda}.lambda_handler"
  source_code_hash = data.archive_file.dummy_transform_lambda.output_base64sha256
  runtime = var.runtime
}

  ## Dummy load lambda

data "archive_file" "dummy_load_lambda" {
    type        = "zip"
    source_file = "${path.module}/../src/dummy_lambda/dummy_${var.load_lambda}.py"
    output_path = "${path.module}/../deployments/dummy_lambda/dummy_lambda_${var.load_lambda}.zip"
}

resource "aws_s3_object" "dummy_load_file_upload" {
  bucket = "${aws_s3_bucket.code_bucket.id}"
  key = "lambda-functions/dummy_lambda/dummy_lambda_${var.load_lambda}.zip"
  source = "${data.archive_file.dummy_load_lambda.output_path}"
}

resource "aws_lambda_function" "dummy_load_lambda" {
  function_name = "dummy_${var.load_lambda}"
  s3_bucket     = "${aws_s3_bucket.code_bucket.id}"
  s3_key        = "${aws_s3_object.dummy_load_file_upload.key}"
  role          = aws_iam_role.iam_role_extraction_lambda.arn
  handler       = "dummy_${var.load_lambda}.lambda_handler"
  source_code_hash = data.archive_file.dummy_load_lambda.output_base64sha256
  runtime = var.runtime
}

