#log group for the extraction lambda
resource "aws_cloudwatch_log_group" "extraction_lambda_log_group"{
    name = "/aws/lambda/${var.extraction_lambda}"
}

resource "aws_sns_topic" "lambda_error_email_alerts" {
    name = "lambda-error-alerts"
}

resource "aws_sns_topic_subscription" "sns_email" {
    topic_arn = aws_sns_topic.lambda_error_email_alerts.arn
    protocol = "email" #how we want to be alerted
    endpoint = "lewisr23491@gmail.com" #email to be used
}

resource "aws_cloudwatch_metric_alarm" "extraction_lambda_alarm"{
    alarm_name = "extraction-lambda-alarm"
    comparison_operator = "GreaterThanThreshold"
    evaluation_periods = 1 #how many periods should the threshold be met before alarm is triggered
    metric_name = "Errors" #the metric that we are monitoring 
    namespace = "AWS/Lambda"
    period = 60 #time in seconds it will be monitored, need to ask group
    statistic = "Sum"
    threshold = 0 #how many errors should we allow before being notified
    alarm_description = "An alarm to log the errors for the extraction lambda"

    dimensions = {
        FunctionName = var.extraction_lambda #linking with the extraction lamnda
    }

    alarm_actions = [aws_sns_topic.lambda_error_email_alerts.arn]
}

