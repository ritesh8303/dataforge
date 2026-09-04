# ai.tf — Thesis AI layer: enrichment + match API
# Separate from main.tf to avoid touching existing pipeline infrastructure.

module "enrichment_lambda" {
  source           = "./modules/lambda"
  function_name    = "dataforge-enrichment"
  handler          = "enrichment_handler.lambda_handler"
  lambda_role_arn  = module.iam.lambda_role_arn
  lambda_role_name = module.iam.lambda_role_name
  source_dir       = "../src"
  layers           = ["arn:aws:lambda:eu-central-1:336392948345:layer:AWSSDKPandas-Python311:12"]
  memory_size      = 2048
  timeout          = 900
  env_vars = {
    GOLD_BUCKET               = module.s3_gold.bucket_id
    GOLD_KEY                  = "all_jobs.csv"
    ENRICHMENT_OUTPUT_KEY     = "ai_job_enrichment.csv"
    EMBEDDING_INDEX_KEY       = "embedding_index.json"
    AI_ENRICHMENT_SAMPLE_RATE = "0.1"
    INDEX_BUILD_LIMIT         = "500"
    AI_ENABLED                = "true"
  }
  bronze_bucket_arn   = module.s3_bronze.arn
  enable_schedule     = true
  schedule_expression = "cron(0 21 * * ? *)" # 21:00 UTC, after Gold (~20:30)
  enable_alerts       = true
  alert_email         = var.alert_email
}

module "match_api_lambda" {
  source           = "./modules/lambda"
  function_name    = "dataforge-match-api"
  handler          = "match_api.lambda_handler"
  lambda_role_arn  = module.iam.lambda_role_arn
  lambda_role_name = module.iam.lambda_role_name
  source_dir       = "../src"
  layers           = ["arn:aws:lambda:eu-central-1:336392948345:layer:AWSSDKPandas-Python311:12"]
  memory_size      = 1024
  timeout          = 60
  env_vars = {
    GOLD_BUCKET         = module.s3_gold.bucket_id
    GOLD_KEY            = "all_jobs.csv"
    ENRICHMENT_KEY      = "ai_job_enrichment.csv"
    EMBEDDING_INDEX_KEY = "embedding_index.json"
    ALLOWED_ORIGIN      = "*"
    AI_ENABLED          = "true"
    INDEX_BUILD_LIMIT   = "200"
  }
  bronze_bucket_arn = module.s3_bronze.arn
  enable_schedule   = false
  enable_alerts     = true
  alert_email       = var.alert_email
}

resource "aws_apigatewayv2_api" "match" {
  name          = "dataforge-match-api"
  protocol_type = "HTTP"
  cors_configuration {
    allow_origins = ["*"]
    allow_methods = ["GET", "POST", "OPTIONS"]
    allow_headers = ["*"]
    max_age       = 300
  }
}

resource "aws_apigatewayv2_integration" "match" {
  api_id                 = aws_apigatewayv2_api.match.id
  integration_type       = "AWS_PROXY"
  integration_uri        = module.match_api_lambda.lambda_function_arn
  payload_format_version = "2.0"
}

resource "aws_apigatewayv2_route" "match_get" {
  api_id    = aws_apigatewayv2_api.match.id
  route_key = "GET /"
  target    = "integrations/${aws_apigatewayv2_integration.match.id}"
}

resource "aws_apigatewayv2_route" "match_post" {
  api_id    = aws_apigatewayv2_api.match.id
  route_key = "POST /"
  target    = "integrations/${aws_apigatewayv2_integration.match.id}"
}

resource "aws_apigatewayv2_stage" "match" {
  api_id      = aws_apigatewayv2_api.match.id
  name        = "$default"
  auto_deploy = true

  default_route_settings {
    throttling_burst_limit = 5
    throttling_rate_limit  = 3
  }
}

resource "aws_lambda_permission" "apigw_match" {
  statement_id  = "AllowAPIGatewayInvokeMatch"
  action        = "lambda:InvokeFunction"
  function_name = module.match_api_lambda.lambda_function_name
  principal     = "apigateway.amazonaws.com"
  source_arn    = "${aws_apigatewayv2_api.match.execution_arn}/*/*"
}

output "match_api_url" {
  description = "Paste into docs/agent.html as MATCH_API_URL"
  value       = aws_apigatewayv2_stage.match.invoke_url
}
