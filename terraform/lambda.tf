
#Extaction lambda

data "archive_file" "extraction_lambda" {
    type        = "zip"
    source_dir = "${path.module}/../src/${var.extraction_lambda}_package"
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
  handler       = "${var.extraction_lambda}.${var.extraction_lambda}_function.lambda_handler"
  source_code_hash = data.archive_file.extraction_lambda.output_base64sha256
  layers        = [aws_lambda_layer_version.layer1.arn]
  runtime       = var.runtime
  timeout = 300
  memory_size = 1024

  environment {
    variables = {
      DB_USER     = var.user
      DB_PASSWORD = var.password
      DB_HOST     = var.host
      DB_PORT     = tostring(var.port)
      DB_NAME     = var.database
    }
  }
}

# transform lambda

data "archive_file" "transform_lambda" {
    type        = "zip"
    source_dir = "${path.module}/../src/${var.transform_lambda}_package"
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
  role          = aws_iam_role.iam_role_transform_lambda.arn 
  handler       = "${var.transform_lambda}.${var.transform_lambda}_function.lambda_handler"
  source_code_hash = data.archive_file.transform_lambda.output_base64sha256
  runtime = var.runtime
  timeout = 300
  memory_size = 1024

  layers = [
    aws_lambda_layer_version.layer2.arn,
    aws_lambda_layer_version.layer1.arn   
  ]

  depends_on = [
    null_resource.create_layer2_dependencies,
    null_resource.create_layer1_dependencies
  ]
}

# Load lambda

data "archive_file" "load_lambda" {
    type        = "zip"
    source_dir = "${path.module}/../src/${var.load_lambda}_package"
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
  role          = aws_iam_role.iam_role_load_lambda.arn 
  handler       = "${var.load_lambda}.${var.load_lambda}_function.lambda_handler"
  source_code_hash = data.archive_file.load_lambda.output_base64sha256
  runtime = var.runtime
  timeout = 300
  memory_size = 1024

  layers = [
    aws_lambda_layer_version.layer2.arn,
    aws_lambda_layer_version.layer1.arn   
  ]

  depends_on = [
    null_resource.create_layer2_dependencies,
    null_resource.create_layer1_dependencies
  ]

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

