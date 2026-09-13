# terraform/stepfunctions.tf
# Thin orchestration proof: EventBridge → Step Functions Express → Silver → Gold.
# Ingest Lambdas stay on their own schedules (cost: Express free tier is ample).
# Optional enrichment step is wired only when ai.tf is present (see commented block).

data "aws_iam_policy_document" "sfn_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["states.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "sfn_pipeline" {
  name               = "dataforge-sfn-pipeline-dev"
  assume_role_policy = data.aws_iam_policy_document.sfn_assume.json
}

data "aws_iam_policy_document" "sfn_invoke" {
  statement {
    sid     = "InvokePipelineLambdas"
    actions = ["lambda:InvokeFunction"]
    resources = [
      module.transformer_lambda.lambda_function_arn,
      module.gold_lambda.lambda_function_arn,
    ]
  }
  statement {
    sid = "CloudWatchLogs"
    actions = [
      "logs:CreateLogDelivery",
      "logs:GetLogDelivery",
      "logs:UpdateLogDelivery",
      "logs:DeleteLogDelivery",
      "logs:ListLogDeliveries",
      "logs:PutResourcePolicy",
      "logs:DescribeResourcePolicies",
      "logs:DescribeLogGroups",
    ]
    resources = ["*"]
  }
}

resource "aws_iam_role_policy" "sfn_pipeline" {
  name   = "dataforge-sfn-invoke"
  role   = aws_iam_role.sfn_pipeline.id
  policy = data.aws_iam_policy_document.sfn_invoke.json
}

resource "aws_sfn_state_machine" "medallion" {
  name     = "dataforge-medallion-express"
  role_arn = aws_iam_role.sfn_pipeline.arn
  type     = "EXPRESS"

  definition = jsonencode({
    Comment = "DataForge medallion thin proof: Silver SCD2 then Gold (ingest stays EventBridge-scheduled)"
    StartAt = "SilverTransform"
    States = {
      SilverTransform = {
        Type     = "Task"
        Resource = "arn:aws:states:::lambda:invoke"
        Parameters = {
          FunctionName = module.transformer_lambda.lambda_function_arn
          Payload = {
            source = "step_functions"
          }
        }
        ResultPath = "$.silver"
        Next       = "GoldGenerate"
        Retry = [
          {
            ErrorEquals     = ["States.TaskFailed", "Lambda.ServiceException"]
            IntervalSeconds = 30
            MaxAttempts     = 2
            BackoffRate     = 2
          }
        ]
      }
      GoldGenerate = {
        Type     = "Task"
        Resource = "arn:aws:states:::lambda:invoke"
        Parameters = {
          FunctionName = module.gold_lambda.lambda_function_arn
          Payload = {
            source = "step_functions"
          }
        }
        ResultPath = "$.gold"
        End        = true
        Retry = [
          {
            ErrorEquals     = ["States.TaskFailed", "Lambda.ServiceException"]
            IntervalSeconds = 30
            MaxAttempts     = 2
            BackoffRate     = 2
          }
        ]
      }
    }
  })
}

# Optional second schedule — keep disabled by default so existing cron Lambdas
# remain the cost-stable path. Enable by setting var.enable_sfn_schedule = true.
resource "aws_cloudwatch_event_rule" "sfn_medallion" {
  count               = var.enable_sfn_schedule ? 1 : 0
  name                = "dataforge-sfn-medallion"
  description         = "Optional Express orchestration of Silver→Gold"
  schedule_expression = "cron(45 20 * * ? *)"
}

resource "aws_iam_role" "events_sfn" {
  count = var.enable_sfn_schedule ? 1 : 0
  name  = "dataforge-events-sfn-dev"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "events.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy" "events_sfn" {
  count = var.enable_sfn_schedule ? 1 : 0
  name  = "start-sfn"
  role  = aws_iam_role.events_sfn[0].id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["states:StartExecution"]
      Resource = [aws_sfn_state_machine.medallion.arn]
    }]
  })
}

resource "aws_cloudwatch_event_target" "sfn_medallion" {
  count     = var.enable_sfn_schedule ? 1 : 0
  rule      = aws_cloudwatch_event_rule.sfn_medallion[0].name
  target_id = "MedallionSFN"
  arn       = aws_sfn_state_machine.medallion.arn
  role_arn  = aws_iam_role.events_sfn[0].arn
}

output "medallion_state_machine_arn" {
  description = "Express state machine for Silver→Gold thin proof (schedule off by default)"
  value       = aws_sfn_state_machine.medallion.arn
}
