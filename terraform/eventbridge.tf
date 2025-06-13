

resource "aws_cloudwatch_event_rule" "run_step_function_every_15_minutes" {
  name                = "run-step-function-every-15-minutes"
  description         = "Rule to trigger every 15 minutes"
  event_bus_name      = data.aws_cloudwatch_event_bus.default.name
  schedule_expression = "cron(0/15 * * * ? *)" // Triggers every 15 minutes
}


resource "aws_cloudwatch_event_target" "trigger_step_function" {
  rule      = aws_cloudwatch_event_rule.run_step_function_every_15_minutes.name
  target_id = "StepFunctionTarget"
  arn       = aws_sfn_state_machine.Step_Function_State_Machine.arn
  role_arn  = aws_iam_role.eventbridge_invoke_step_function_role.arn
}

data "aws_cloudwatch_event_bus" "default" {
   name = "default"
}



