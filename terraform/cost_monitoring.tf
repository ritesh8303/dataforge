# Cost monitoring: AWS Budgets + CloudWatch billing alarms (us-east-1).
# Billing metrics only exist in us-east-1 for commercial AWS.

variable "monthly_budget_usd" {
  description = "Monthly cost budget in USD (AWS Budgets unit; EUR card is charged the converted amount)"
  type        = string
  default     = "1"
}

variable "billing_alarm_usd" {
  description = "CloudWatch EstimatedCharges alarm threshold in USD"
  type        = number
  default     = 1
}

variable "billing_alarm_high_usd" {
  description = "Second CloudWatch EstimatedCharges alarm for a hard ceiling"
  type        = number
  default     = 5
}

provider "aws" {
  alias  = "us_east_1"
  region = "us-east-1"
}

resource "aws_sns_topic" "cost_alerts" {
  provider = aws.us_east_1
  name     = "dataforge-cost-alerts"
}

resource "aws_sns_topic_subscription" "cost_alerts_email" {
  provider  = aws.us_east_1
  topic_arn = aws_sns_topic.cost_alerts.arn
  protocol  = "email"
  endpoint  = var.alert_email
}

# Requires Billing console → Billing preferences → "Receive billing alerts" enabled.
resource "aws_cloudwatch_metric_alarm" "estimated_charges" {
  provider            = aws.us_east_1
  alarm_name          = "dataforge-estimated-charges-${var.billing_alarm_usd}-usd"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = 1
  metric_name         = "EstimatedCharges"
  namespace           = "AWS/Billing"
  period              = 21600
  statistic           = "Maximum"
  threshold           = var.billing_alarm_usd
  treat_missing_data  = "notBreaching"
  alarm_description   = "DataForge estimated charges >= ${var.billing_alarm_usd} USD"
  alarm_actions       = [aws_sns_topic.cost_alerts.arn]
  ok_actions          = [aws_sns_topic.cost_alerts.arn]

  dimensions = {
    Currency = "USD"
  }
}

resource "aws_cloudwatch_metric_alarm" "estimated_charges_high" {
  provider            = aws.us_east_1
  alarm_name          = "dataforge-estimated-charges-${var.billing_alarm_high_usd}-usd"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = 1
  metric_name         = "EstimatedCharges"
  namespace           = "AWS/Billing"
  period              = 21600
  statistic           = "Maximum"
  threshold           = var.billing_alarm_high_usd
  treat_missing_data  = "notBreaching"
  alarm_description   = "DataForge estimated charges >= ${var.billing_alarm_high_usd} USD (hard ceiling)"
  alarm_actions       = [aws_sns_topic.cost_alerts.arn]

  dimensions = {
    Currency = "USD"
  }
}

output "cost_alerts_sns_topic_arn" {
  value = aws_sns_topic.cost_alerts.arn
}
