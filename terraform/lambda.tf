data "archive_file" "lambda" {
    type        = "zip"
    source_file = "${path.module}/../src/lambda/terraform-placeholder-change-detection-lambda.py"
    output_path = "${path.module}/../deployments/change-detection-lambda.zip"
}


resource "aws_lambda_function" "change_detection_lambda" {
  # If the file is not in the current working directory you will need to include a
  # path.module in the filename.
  filename      = "${path.module}/../deployments/change-detection-lambda.zip"
  # function_name is the lambda function name as it will appear in AWS
  function_name = "change_detection_lambda"
  role          = aws_iam_role.iam_for_lambda_change_detection.arn
  handler       = "terraform-placeholder-change-detection-lambda.lambda_handler"

  source_code_hash = data.archive_file.lambda.output_base64sha256

  runtime = "python3.13"

    # environment {
    #     variables = {
    #     foo = "bar"
    #     }
    # }
}