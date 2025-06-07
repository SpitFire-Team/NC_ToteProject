

resource "aws_cloudwatch_event_rule" "run_step_function_every_15_minutes" {
  name                = "run-step-function-every-15-minutes"
  description         = "Rule to trigger every 15 minutes"
  event_bus_name      = data.aws_cloudwatch_event_bus.default.name
  schedule_expression = "cron(0/15 * * * ? *)" // Triggers every 15 minutes
}

resource "aws_cloudwatch_event_target" "trigger_step_function" {
  rule      = aws_cloudwatch_event_rule.run_step_function_every_15_minutes.name
  target_id = var.step_function
  arn       = aws_sfn_state_machine.StepFunctionsStateMachine.arn
  role_arn  = aws_iam_role.eventbridge_invoke_step_function_role.arn
}

# IAM role assumed by CloudWatch Events
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

# Attach the policy document to the role
resource "aws_iam_role_policy" "eventbridge_policy" {
  name   = "allow-start-step-function"
  role   = aws_iam_role.eventbridge_invoke_step_function_role.id
  policy = data.aws_iam_policy_document.start_step_function.json
}


data "aws_cloudwatch_event_bus" "default" {
   name = "default"
}

# resource "aws_cloudwatch_event_rule" "run_extraction_lambda_every_15_minutes" {
#   name                = "run-extraction-lambda-every-15-minutes"
#   description         = "Rule to trigger every 15 minutes"
#   event_bus_name      = data.aws_cloudwatch_event_bus.default.name
#   schedule_expression = "cron(0/15 * * * ? *)" // Triggers every 15 minutes
# }

# resource "aws_cloudwatch_event_target" "run_extraction_lambda_every_15_minutes_targer" {
#     rule = aws_cloudwatch_event_rule.run_extraction_lambda_every_15_minutes.name
#     target_id = var.extraction_lambda
#     arn = aws_lambda_function.extraction_lambda.arn
# }




# resource "aws_lambda_permission" "allow_cloudwatch_to_call_extraction_lambda" {
#     statement_id = "AllowExecutionFromCloudWatch"
#     action = "lambda:InvokeFunction"
#     function_name = aws_lambda_function.extraction_lambda.function_name
#     principal = "events.amazonaws.com"
#     source_arn = aws_cloudwatch_event_rule.run_extraction_lambda_every_15_minutes.arn
# }



