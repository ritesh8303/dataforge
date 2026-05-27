# metrics.tf — Dashboard Metrics API
# Separate from main.tf to avoid touching existing pipeline infrastructure.
# Reuses the shared IAM role (already has s3:GetObject on Gold bucket).

module "metrics_lambda" {
  source           = "./modules/lambda"
  function_name    = "dataforge-metrics"
  handler          = "metrics_api.lambda_handler"
  lambda_role_arn  = module.iam.lambda_role_arn
  lambda_role_name = module.iam.lambda_role_name
  source_dir       = "../src"
  layers           = ["arn:aws:lambda:eu-central-1:336392948345:layer:AWSSDKPandas-Python311:12"]
  memory_size      = 256
  timeout          = 30
  env_vars = {
    GOLD_BUCKET    = module.s3_gold.bucket_id
    ALLOWED_ORIGIN = var.dashboard_origin
  }
  # No S3 trigger, no schedule — invoked only via Function URL
  bronze_bucket_arn = module.s3_bronze.arn
  enable_schedule   = false
  enable_alerts     = true
  alert_email       = var.alert_email
}

# API Gateway HTTP API — no account-level public access block, $0 cost within free tier
resource "aws_apigatewayv2_api" "metrics" {
  name          = "dataforge-metrics-api"
  protocol_type = "HTTP"
  cors_configuration {
    allow_origins = ["*"]
    allow_methods = ["GET", "OPTIONS"]
    allow_headers = ["*"]
    max_age       = 300
  }
}

resource "aws_apigatewayv2_integration" "metrics" {
  api_id                 = aws_apigatewayv2_api.metrics.id
  integration_type       = "AWS_PROXY"
  integration_uri        = module.metrics_lambda.lambda_function_arn
  payload_format_version = "2.0"
}

resource "aws_apigatewayv2_route" "metrics" {
  api_id    = aws_apigatewayv2_api.metrics.id
  route_key = "GET /"
  target    = "integrations/${aws_apigatewayv2_integration.metrics.id}"
}

resource "aws_apigatewayv2_stage" "metrics" {
  api_id      = aws_apigatewayv2_api.metrics.id
  name        = "$default"
  auto_deploy = true
}

resource "aws_lambda_permission" "apigw_metrics" {
  statement_id  = "AllowAPIGatewayInvoke"
  action        = "lambda:InvokeFunction"
  function_name = module.metrics_lambda.lambda_function_name
  principal     = "apigateway.amazonaws.com"
  source_arn    = "${aws_apigatewayv2_api.metrics.execution_arn}/*/*"
}

output "metrics_function_url" {
  description = "Paste this URL into docs/index.html as METRICS_API_URL"
  value       = aws_apigatewayv2_stage.metrics.invoke_url
}
