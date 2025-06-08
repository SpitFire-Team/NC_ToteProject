### lambda

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

## extraction Lamdba

resource "aws_iam_role" "iam_role_extraction_lambda" {
  name_prefix        = "role-${var.extraction_lambda}-"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume_role.json
}

data "aws_iam_policy_document" "s3_extraction_permissions_document" {
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

resource "aws_iam_policy" "s3_extraction_policy" {
  name_prefix = "s3-policy-${var.extraction_lambda}-"
  policy      = data.aws_iam_policy_document.s3_extraction_permissions_document.json
}

resource "aws_iam_role_policy_attachment" "lambda_s3_extraction_policy_attachment" {
  role        = aws_iam_role.iam_role_extraction_lambda.name
  policy_arn  = aws_iam_policy.s3_extraction_policy.arn
}

## transform Lamdba

resource "aws_iam_role" "iam_role_transform_lambda" {
  name_prefix        = "role-${var.transform_lambda}-"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume_role.json
}

data "aws_iam_policy_document" "s3_transform_permissions_document" {
  statement   {
    actions   = [
        "s3:GetObject",
        "s3:PutObject",
    ]

    resources = [
      "${aws_s3_bucket.processed_bucket.arn}/*",
    ]
  }
}

resource "aws_iam_policy" "s3_transform_policy" {
  name_prefix = "s3-policy-${var.transform_lambda}-"
  policy      = data.aws_iam_policy_document.s3_transform_permissions_document.json
}

resource "aws_iam_role_policy_attachment" "lambda_s3_transform_policy_attachment" {
  role        = aws_iam_role.iam_role_transform_lambda.name
  policy_arn  = aws_iam_policy.s3_transform_policy.arn
}


## load Lamdba

resource "aws_iam_role" "iam_role_load_lambda" {
  name_prefix        = "role-${var.load_lambda}-"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume_role.json
}


### Eventbridge 

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
      aws_sfn_state_machine.Step_Function_State_Machine.arn
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

### step_functions IAM roles 

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

  ## dummy_step_function_role - to clean

resource "aws_iam_role" "dummy_step_function_role" {
  name               = "${var.dummy_step_function_name}-role"
  assume_role_policy = data.aws_iam_policy_document.step_function_assume_role.json
}

data "aws_iam_policy_document" "dummy_step_function_permissions" {
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

resource "aws_iam_role_policy" "dummy_step_function_policy" {
  name   = "${var.dummy_step_function_name}-policy"
  role   = aws_iam_role.dummy_step_function_role.id
  policy = data.aws_iam_policy_document.dummy_step_function_permissions.json
}



  ## step_function_role 

resource "aws_iam_role" "step_function_role" {
  name               = "${var.step_function_name}-role"
  assume_role_policy = data.aws_iam_policy_document.step_function_assume_role.json
}

data "aws_iam_policy_document" "step_function_permissions" {
  statement {
    effect = "Allow"
    actions = ["lambda:InvokeFunction"]
    resources = [
      "${aws_lambda_function.extraction_lambda.arn}:*",
      "${aws_lambda_function.transform_lambda.arn}:*",
      "${aws_lambda_function.load_lambda.arn}:*"
    ]
  }
}

resource "aws_iam_role_policy" "step_function_policy" {
  name   = "${var.step_function_name}-policy"
  role   = aws_iam_role.step_function_role.id
  policy = data.aws_iam_policy_document.step_function_permissions.json
}
