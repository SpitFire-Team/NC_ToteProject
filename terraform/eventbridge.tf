resource "aws_cloudwatch_event_rule" "15 minute scheduler" {
  name                = "every_15_minute_schulder"
  description         = "Rule to trigger every 15 minutes"
  event_bus_name      = data.aws_cloudwatch_event_bus.default.name
  schedule_expression = "cron(0/15 * * * ? *)" // Triggers every 15 minutes

  target {
    arn      = data.aws_cloudwatch_event_bus.default.arn
    role_arn = aws_iam_role.scheduler.arn
    # eventbridge_parameters {
    #   detail_type = "My Scheduler"
    #   source      = "Custom Scheduler"
    # }
  }
}

data "aws_cloudwatch_event_bus" "default" {
  name =   "${var.extraction_lambda_eventbus.default}"
}



# resource "aws_lambda_function" "check_foo" {
#     filename = "check_foo.zip"
#     function_name = "checkFoo"
#     role = "arn:aws:iam::424242:role/something"
#     handler = "index.handler"
# }

# resource "aws_cloudwatch_event_rule" "every_five_minutes" {
#     name = "every-five-minutes"
#     description = "Fires every five minutes"
#     schedule_expression = "rate(5 minutes)"
# }

resource "aws_cloudwatch_event_target" "check_foo_every_five_minutes" {
    rule = aws_cloudwatch_event_rule.every_five_minutes.name
    target_id = "check_foo"
    arn = aws_lambda_function.check_foo.arn
}

resource "aws_lambda_permission" "allow_cloudwatch_to_call_check_foo" {
    statement_id = "AllowExecutionFromCloudWatch"
    action = "lambda:InvokeFunction"
    function_name = aws_lambda_function.check_foo.function_name
    principal = "events.amazonaws.com"
    source_arn = aws_cloudwatch_event_rule.every_five_minutes.arn
}


# resource "aws_scheduler_schedule" "better_scheduler" {
#   name = "better_scheduler"
#   flexible_time_window {
#     mode = "OFF"
#   }
#   target {
#     arn      = data.aws_cloudwatch_event_bus.default.arn
#     role_arn = aws_iam_role.scheduler.arn
#     eventbridge_parameters {
#       detail_type = "My Scheduler"
#       source      = "Custom Scheduler"
#     }

#     // Event Payload (if required)
#     input = jsonencode({
#       Message = "Super Schedule"
#     })
#   }

#   schedule_expression = "cron(* * * * ? *)" // Triggers every minute, could also be rate(1 minute)
# }