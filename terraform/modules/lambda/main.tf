data "archive_file" "lambda_zip" {
  type        = "zip"
  source_dir  = var.source_dir
  output_path = "${path.module}/${var.function_name}.zip"
}

resource "aws_lambda_function" "this" {
  filename         = data.archive_file.lambda_zip.output_path
  function_name    = var.function_name
  role             = var.lambda_role_arn
  handler          = var.handler
  runtime          = "python3.11"
  source_code_hash = data.archive_file.lambda_zip.output_base64sha256
  timeout          = 30
  
  # NEW: Handles the memory (Set to 512 in your root main.tf)
  memory_size      = var.memory_size

  dynamic "dead_letter_config" {
    for_each = var.dlq_target_arn != null ? [1] : []
    content {
      target_arn = var.dlq_target_arn
    }
  }

  # Handles the layers (DuckDB + Pandas)
  layers           = var.layers

  environment {
    # Automatically inject SNS ARN if alerts are enabled so Python can find it
    variables = merge(var.env_vars, 
      var.enable_alerts ? { SNS_TOPIC_ARN = aws_sns_topic.alerts[0].arn } : {}
    )
  }
}

# Explicitly manage CloudWatch Logs for the Lambda
resource "aws_cloudwatch_log_group" "this" {
  name              = "/aws/lambda/${var.function_name}"
  retention_in_days = 7
}

# Permissions to allow S3 to trigger the Lambda
resource "aws_lambda_permission" "allow_s3" {
  statement_id  = "AllowS3Invoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.this.function_name
  principal     = "s3.amazonaws.com"
  source_arn    = var.bronze_bucket_arn
}

# NEW: EventBridge rule to trigger Ingestion daily (Free Tier)
resource "aws_cloudwatch_event_rule" "daily_trigger" {
  count               = var.enable_schedule ? 1 : 0
  name                = "${var.function_name}-daily-schedule"
  description         = "Triggers the ingestion Lambda every day at 8 AM"
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

# NEW: SQS Queue for Dead Letter Queue (DLQ) for the process_data Lambda
# This resource should ideally be defined in your root main.tf and its ARN passed in.
# For simplicity within this module context, we'll create it only for the 'process-data' function.
resource "aws_sqs_queue" "lambda_dlq" {
  count = var.enable_alerts ? 1 : 0
  name  = "${var.function_name}-dlq"
  message_retention_seconds = 1209600 # 14 days (max)
}

# NEW: IAM Policy for Lambda to send messages to SQS DLQ
# This policy needs to be attached to the Lambda's execution role.
resource "aws_iam_role_policy" "lambda_dlq_policy" {
  count = var.enable_alerts ? 1 : 0
  name  = "${var.function_name}-dlq-policy"
  role  = var.lambda_role_name

  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Action = [
          "sqs:SendMessage"
        ],
        Effect   = "Allow",
        Resource = aws_sqs_queue.lambda_dlq[0].arn
      }
    ]
  })
}

# NEW: SNS Topic for Alerts (Free Tier allows 1 million mobile/email notifications)
# This resource should ideally be defined in your root main.tf and its ARN passed in.
# For simplicity within this module context, we'll create it only for the 'process-data' function.
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
