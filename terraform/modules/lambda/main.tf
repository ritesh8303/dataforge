data "archive_file" "lambda_zip" {
  type        = "zip"
  source_dir  = var.source_dir
  output_path = "${path.module}/${var.function_name}.zip"
}

# SQS DLQ — created for all Lambdas, wired automatically
resource "aws_sqs_queue" "lambda_dlq" {
  name                      = "${var.function_name}-dlq"
  message_retention_seconds = 1209600 # 14 days
}

resource "aws_iam_role_policy" "lambda_dlq_policy" {
  name = "${var.function_name}-dlq-policy"
  role = var.lambda_role_name

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["sqs:SendMessage"]
      Resource = aws_sqs_queue.lambda_dlq.arn
    }]
  })
}

resource "aws_lambda_function" "this" {
  filename         = data.archive_file.lambda_zip.output_path
  function_name    = var.function_name
  role             = var.lambda_role_arn
  handler          = var.handler
  runtime          = "python3.11"
  source_code_hash = data.archive_file.lambda_zip.output_base64sha256
  timeout          = var.timeout
  memory_size      = var.memory_size
  layers           = var.layers

  dead_letter_config {
    target_arn = aws_sqs_queue.lambda_dlq.arn
  }

  environment {
    variables = merge(
      var.env_vars,
      var.enable_alerts ? { SNS_TOPIC_ARN = aws_sns_topic.alerts[0].arn } : {}
    )
  }
}

resource "aws_cloudwatch_log_group" "this" {
  name              = "/aws/lambda/${var.function_name}"
  retention_in_days = 7
}

# CloudWatch alarm — triggers SNS when Lambda errors > 0
resource "aws_cloudwatch_metric_alarm" "lambda_errors" {
  count               = var.enable_alerts ? 1 : 0
  alarm_name          = "${var.function_name}-errors"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "Errors"
  namespace           = "AWS/Lambda"
  period              = 300
  statistic           = "Sum"
  threshold           = 0
  alarm_description   = "Lambda ${var.function_name} has errors"
  alarm_actions       = [aws_sns_topic.alerts[0].arn]

  dimensions = {
    FunctionName = aws_lambda_function.this.function_name
  }
}

# SNS topic + email subscription for alerts
resource "aws_sns_topic" "alerts" {
  count = var.enable_alerts ? 1 : 0
  name  = "${var.function_name}-alerts"
}

resource "aws_sns_topic_subscription" "email_alert" {
  count     = var.enable_alerts ? 1 : 0
  topic_arn = aws_sns_topic.alerts[0].arn
  protocol  = "email"
  endpoint  = var.alert_email
}

# Allow S3 to invoke this Lambda
resource "aws_lambda_permission" "allow_s3" {
  statement_id  = "AllowS3Invoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.this.function_name
  principal     = "s3.amazonaws.com"
  source_arn    = var.bronze_bucket_arn
}

# EventBridge daily schedule
resource "aws_cloudwatch_event_rule" "daily_trigger" {
  count               = var.enable_schedule ? 1 : 0
  name                = "${var.function_name}-daily-schedule"
  description         = "Triggers ${var.function_name} every day at 8 AM UTC"
  schedule_expression = "cron(0 8 * * ? *)"
}

resource "aws_cloudwatch_event_target" "lambda_target" {
  count     = var.enable_schedule ? 1 : 0
  rule      = aws_cloudwatch_event_rule.daily_trigger[0].name
  target_id = "SendToLambda"
  arn       = aws_lambda_function.this.arn
}

resource "aws_lambda_permission" "allow_eventbridge" {
  count         = var.enable_schedule ? 1 : 0
  statement_id  = "AllowExecutionFromEventBridge"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.this.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.daily_trigger[0].arn
}
