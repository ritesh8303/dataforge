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

# Lambda Function URL — public HTTPS endpoint, no API Gateway needed ($0 cost)
resource "aws_lambda_function_url" "metrics" {
  function_name      = module.metrics_lambda.lambda_function_name
  authorization_type = "NONE"

  cors {
    allow_credentials = false
    allow_origins     = [var.dashboard_origin]
    allow_methods     = ["*"]
    allow_headers     = ["Content-Type"]
    max_age           = 300
  }
}

# Allows anyone to invoke via the Function URL (required even with AUTH_TYPE=NONE)
resource "aws_lambda_permission" "metrics_url_public" {
  statement_id           = "AllowPublicFunctionURL"
  action                 = "lambda:InvokeFunctionUrl"
  function_name          = module.metrics_lambda.lambda_function_name
  principal              = "*"
  function_url_auth_type = "NONE"
}

output "metrics_function_url" {
  description = "Paste this URL into docs/index.html as METRICS_API_URL"
  value       = aws_lambda_function_url.metrics.function_url
}
