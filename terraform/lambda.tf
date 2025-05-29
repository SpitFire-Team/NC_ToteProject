data "archive_file" "lambda" {
    type        = "zip"
    source_file = "${path.module}/../src/lambda/${var.extraction_lambda}.py"
    output_path = "${path.module}/../deployments/${var.extraction_lambda}.zip"
}

resource "aws_lambda_function" "extraction_lambda" {
  function_name = "${var.extraction_lambda}"
  filename      = "${path.module}/../deployments/${var.extraction_lambda}.zip"
  role          = aws_iam_role.iam_role_extraction_lambda.arn
  handler       = "${var.extraction_lambda}.lambda_handler"
  source_code_hash = data.archive_file.lambda.output_base64sha256
  runtime = "python3.13"
}

