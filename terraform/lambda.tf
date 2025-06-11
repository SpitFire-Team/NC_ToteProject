
#Extaction lambda

data "archive_file" "extraction_lambda" {
    type        = "zip"
    source_dir = "${path.module}/../src/${var.extraction_lambda}"
    output_path = "${path.module}/../deployments/${var.extraction_lambda}.zip"
}

resource "aws_s3_object" "extaction_file_upload" {
  bucket = "${aws_s3_bucket.code_bucket.id}"
  key = "lambda-functions/${var.extraction_lambda}.zip"
  source = "${data.archive_file.extraction_lambda.output_path}"
}


resource "aws_lambda_function" "extraction_lambda" {
  function_name = "${var.extraction_lambda}"
  s3_bucket     = "${aws_s3_bucket.code_bucket.id}"
  s3_key        = "${aws_s3_object.extaction_file_upload.key}"
  role          = aws_iam_role.iam_role_extraction_lambda.arn
  handler       = "${var.extraction_lambda}.lambda_handler"
  # handler = "extraction_lambda.extraction_lambda.lambda_handler" 
  source_code_hash = data.archive_file.extraction_lambda.output_base64sha256
  layers        = [aws_lambda_layer_version.layer.arn]
  runtime       = var.runtime
  timeout = 300
  memory_size = 1024

  environment {
    variables = {
      USER     = var.user
      PASSWORD = var.password
      HOST     = var.host
      PORT     = tostring(var.port)
      DATABASE = var.database
    }
  }
}

# transform lambda

data "archive_file" "transform_lambda" {
    type        = "zip"
    source_dir = "${path.module}/../src/${var.transform_lambda}"
    output_path = "${path.module}/../deployments/${var.transform_lambda}.zip"
}

resource "aws_s3_object" "transform_file_upload" {
  bucket = "${aws_s3_bucket.code_bucket.id}"
  key = "lambda-functions/${var.transform_lambda}.zip"
  source = "${data.archive_file.transform_lambda.output_path}"
}

resource "aws_lambda_function" "transform_lambda" {
  function_name = "${var.transform_lambda}"
  s3_bucket     = "${aws_s3_bucket.code_bucket.id}"
  s3_key        = "${aws_s3_object.transform_file_upload.key}"
  role          = aws_iam_role.iam_role_transform_lambda.arn  ## complete this in IAM
  handler       = "${var.transform_lambda}.lambda_handler"
  source_code_hash = data.archive_file.transform_lambda.output_base64sha256
  layers = [aws_lambda_layer_version.layer.arn]
  runtime = var.runtime
  timeout = 300
  memory_size = 1024
}

# Load lambda

data "archive_file" "load_lambda" {
    type        = "zip"
    source_dir = "${path.module}/../src/${var.load_lambda}"
    output_path = "${path.module}/../deployments/${var.load_lambda}.zip"
}

resource "aws_s3_object" "load_file_upload" {
  bucket = "${aws_s3_bucket.code_bucket.id}"
  key = "lambda-functions/${var.load_lambda}.zip"
  source = "${data.archive_file.load_lambda.output_path}"
}

resource "aws_lambda_function" "load_lambda" {
  function_name = "${var.load_lambda}"
  s3_bucket     = "${aws_s3_bucket.code_bucket.id}"
  s3_key        = "${aws_s3_object.load_file_upload.key}"
  role          = aws_iam_role.iam_role_load_lambda.arn  ## complete this in IAM
  handler       = "${var.load_lambda}.lambda_handler"
  source_code_hash = data.archive_file.load_lambda.output_base64sha256
  layers = [aws_lambda_layer_version.layer.arn]
  runtime = var.runtime
  timeout = 300
  memory_size = 1024

  #Added environment variables  - Note - should change for final data base!!!
  environment {
    variables = {
      USER     = var.user_load
      PASSWORD = var.password_load
      HOST     = var.host_load
      PORT     = tostring(var.port_load)
      DATABASE = var.database_load
    }
  }
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

  ## Dummy transform lambda

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

/*
To run the terraform, its needed to export environment variables with the following console commands:

export TF_VAR_user=
export TF_VAR_password=
export TF_VAR_host=
export TF_VAR_port=5432
export TF_VAR_database=

and the database creditials for the load data base

export TF_VAR_user_load=
export TF_VAR_password_load=
export TF_VAR_host_load=
export TF_VAR_port_load=5432
export TF_VAR_database_load=

(insert the database credentials at the end of the variables above)
*/

