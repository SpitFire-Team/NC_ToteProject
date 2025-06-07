### lambda


resource "aws_iam_role" "iam_role_extraction_lambda" {
  name_prefix        = "role-extraction-lambda-"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume_role.json
}

data "aws_iam_policy_document" "lambda_assume_role" {
  statement {
    effect = "Allow"

    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }

    actions = ["sts:AssumeRole"]
  }
}

data "aws_iam_policy_document" "s3_permissions_document" {
  statement   {
    actions   = [
        "s3:GetObject",
        "s3:PutObject",
    ]

    resources = [
      "${aws_s3_bucket.ingestion_bucket.arn}/*",
    ]
  }
}

resource "aws_iam_policy" "s3_policy" {
  name_prefix = "s3-policy-${var.extraction_lambda}-"
  policy      = data.aws_iam_policy_document.s3_permissions_document.json
}

resource "aws_iam_role_policy_attachment" "lambda_s3_policy_attachment" {
  role        = aws_iam_role.iam_role_extraction_lambda.name
  policy_arn  = aws_iam_policy.s3_policy.arn
}


### Eventbridge 

# resource "aws_iam_role" "extraction_lambda_scheduler_role" {
#   name               = "extraction-lambda-scheduler-role"
#   assume_role_policy = data.aws_iam_policy_document.eventbridge_assume_policy.json
# }

resource "aws_iam_role" "eventbridge_invoke_step_function_role" {
  name = "eventbridge_invoke_step_function"

  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [{
      Effect = "Allow",
      Principal = {
        Service = "events.amazonaws.com"
      },
      Action = "sts:AssumeRole"
    }]
  })
}

# IAM policy document allowing EventBridge to StartExecution
data "aws_iam_policy_document" "start_step_function" {
  statement {
    effect = "Allow"

    actions = [
      "states:StartExecution"
    ]

    resources = [
      aws_sfn_state_machine.StepFunctionsStateMachine.arn
    ]
  }
}

resource "aws_iam_role_policy" "start_execution_policy" {
  name   = "start-step-function"
  role   = aws_iam_role.eventbridge_invoke_step_function_role.id
  policy = data.aws_iam_policy_document.start_step_function.json
}


### Cloudwatch
resource "aws_iam_policy" "cloudwatch_log_policy"{
  name_prefix = "cloudwatch-logs-policy-${var.extraction_lambda}-"
  policy = data.aws_iam_policy_document.cloudwatch_logs_policy_document.json
}

data "aws_iam_policy_document" "cloudwatch_logs_policy_document"{
  statement {
    effect = "Allow"

    actions = [
      "logs:CreateLogGroup",
      "logs:CreateLogStream",
      "logs:PutLogEvents"
    ]

    resources = ["arn:aws:logs:*:*:*"] #which resources should be allowed
  }
}

resource "aws_iam_role_policy_attachment" "lambda_cloudwatch_logs_policy_attachment" {
  role = aws_iam_role.iam_role_extraction_lambda.name
  policy_arn = aws_iam_policy.cloudwatch_log_policy.arn
}

## IAM roles for step_functions

data "aws_iam_policy_document" "step_function_assume_role" {
  statement {
    effect = "Allow"

    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["states.amazonaws.com"]
    }

    sid = "StepFunctionAssumeRole"
  }
}

resource "aws_iam_role" "step_function_role" {
  name               = "${var.step_function_name}-role"
  assume_role_policy = data.aws_iam_policy_document.step_function_assume_role.json
}

data "aws_iam_policy_document" "step_function_permissions" {
  statement {
    effect = "Allow"
    actions = ["lambda:InvokeFunction"]
    resources = [
      "${aws_lambda_function.dummy_extraction_lambda.arn}:*",
      "${aws_lambda_function.dummy_transform_lambda.arn}:*",
      "${aws_lambda_function.dummy_load_lambda.arn}:*"
    ]
  }
}

resource "aws_iam_role_policy" "step_function_policy" {
  name   = "${var.step_function_name}-policy"
  role   = aws_iam_role.step_function_role.id
  policy = data.aws_iam_policy_document.step_function_permissions.json
}

