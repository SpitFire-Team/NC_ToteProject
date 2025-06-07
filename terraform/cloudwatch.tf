#log group for the extraction lambda
resource "aws_cloudwatch_log_group" "extraction_lambda_log_group"{
    name = "/aws/lambda/${var.extraction_lambda}"
}

resource "aws_cloudwatch_log_metric_filter" "extraction_lambda_log_group" {
  name           = "ErrorFilter" #A name for the metric filter.
  pattern        = "ERROR" #CloudWatch Logs filter pattern. 
  #Attention! We need to discuss with the group which level of logger to choose above. 
  #We are between CRITICAL and ERROR, for clarification: https://docs.python.org/3/howto/logging.html
  log_group_name = aws_cloudwatch_log_group.extraction_lambda_log_group.name 

  metric_transformation {
    name      = "Error_Count" #The name of the CloudWatch metric to which the monitored log information should be published 
    namespace = "Extraction_Lambda/Error_Count" #The destination namespace of the CloudWatch metric.
    value     = "1" #What to publish to the metric. EG: 1 for each error. 
  }
}


resource "aws_sns_topic" "lambda_error_email_alerts" {
    name = "lambda-error-alerts"
}

resource "aws_sns_topic_subscription" "sns_email" {
    topic_arn = aws_sns_topic.lambda_error_email_alerts.arn
    protocol = "email" #how we want to be alerted
    endpoint = "spitfiretotes@gmail.com" #email to be used
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







