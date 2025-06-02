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

resource "aws_iam_role" "extraction_lambda_scheduler_role" {
  name               = "extraction-lambda-scheduler-role"
  assume_role_policy = data.aws_iam_policy_document.eventbridge_assume_policy.json
}

data "aws_iam_policy_document" "eventbridge_assume_policy" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["scheduler.amazonaws.com"]
    }
  }
}



