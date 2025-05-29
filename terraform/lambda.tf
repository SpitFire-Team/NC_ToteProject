data "archive_file" "lambda" {
    type        = "zip"
    source_file = "${path.module}/../src/lambda/${var.change_detection_lambda}.py"
    output_path = "${path.module}/../deployments/${var.change_detection_lambda}.zip"
}

resource "aws_lambda_function" "change_detection_lambda" {
  function_name = "${var.change_detection_lambda}"
  filename      = "${path.module}/../deployments/${var.change_detection_lambda}.zip"
  role          = aws_iam_role.iam_role_lambda_change_detection.arn
  handler       = "${var.change_detection_lambda}.lambda_handler"
  source_code_hash = data.archive_file.lambda.output_base64sha256
  runtime = "python3.13"
}


