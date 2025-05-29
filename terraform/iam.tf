resource "aws_iam_role" "iam_role_lambda_change_detection" {
  # change the name_prefix if the name of the lambda changes
  name_prefix        = "role-change-detection-lambda-"
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

# Shall we also put s3:PutObject in?
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
  name_prefix = "s3-policy-${var.change_detection_lambda}-"
  policy      = data.aws_iam_policy_document.s3_permissions_document.json
}

resource "aws_iam_role_policy_attachment" "lambda_s3_policy_attachment" {
  role        = aws_iam_role.iam_role_lambda_change_detection.name
  policy_arn  = aws_iam_policy.s3_policy.arn
}



