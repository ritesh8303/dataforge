# main.tf
terraform {
  required_version = ">= 1.6"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
  backend "s3" {
    bucket         = "dataforge-terraform-state-942361733704"
    key            = "dataforge/terraform.tfstate"
    region         = "eu-central-1"
    dynamodb_table = "dataforge-terraform-locks"
    encrypt        = true
  }
}

provider "aws" {
  region = "eu-central-1"
}

module "s3_bronze" {
  source      = "./modules/s3"
  bucket_name = "dataforge-bronze-dev-eu-central-1"
}

module "s3_silver" {
  source      = "./modules/s3"
  bucket_name = "dataforge-silver-dev-eu-central-1"
}

module "s3_gold" {
  source      = "./modules/s3"
  bucket_name = "dataforge-gold-dev-eu-central-1"
}

# --- 2. PERMISSIONS & SECURITY (Free SSM Tier) ---
module "iam" {
  source       = "./modules/iam"
  project_name = "dataforge"
  environment  = "dev"
}

# BA API is fully public — no credentials needed
# SSM Standard String is free (SecureString costs $0.05/month)
resource "aws_ssm_parameter" "ba_api_keys" {
  name        = "/dataforge/dev/ba_api_credentials"
  description = "BA API public key (no auth required)"
  type        = "String"
  value       = "jobboerse-jobsuche"
}

# --- 3. COMPUTE LAYER ---

# Arbeitnow Ingestor
module "ingestion_lambda" {
  source           = "./modules/lambda"
  function_name    = "dataforge-ingestor"
  handler          = "ingest_arbeitnow.lambda_handler"
  lambda_role_arn  = module.iam.lambda_role_arn
  lambda_role_name = module.iam.lambda_role_name
  source_dir       = "../src"
  layers           = ["arn:aws:lambda:eu-central-1:336392948345:layer:AWSSDKPandas-Python311:12"]
  memory_size      = 512
  timeout          = 300
  env_vars = {
    BRONZE_BUCKET      = module.s3_bronze.bucket_id
    SSM_PARAMETER_NAME = aws_ssm_parameter.ba_api_keys.name
  }
  bronze_bucket_arn = module.s3_bronze.arn
  enable_schedule   = true
  alert_email       = var.alert_email
}

# BA (Federal) Ingestor
module "ba_ingestor" {
  source           = "./modules/lambda"
  function_name    = "dataforge-ba-ingestor"
  handler          = "ingest_ba_api.lambda_handler"
  lambda_role_arn  = module.iam.lambda_role_arn
  lambda_role_name = module.iam.lambda_role_name
  source_dir       = "../src"
  env_vars = {
    BRONZE_BUCKET      = module.s3_bronze.bucket_id
    SSM_PARAMETER_NAME = aws_ssm_parameter.ba_api_keys.name
  }
  layers          = ["arn:aws:lambda:eu-central-1:336392948345:layer:AWSSDKPandas-Python311:12"]
  memory_size     = 512
  timeout         = 300
  enable_schedule = true
  alert_email     = var.alert_email
}

# Company Careers Direct Ingestor (Greenhouse + Lever + Workable + SmartRecruiters)
module "company_ingestor" {
  source           = "./modules/lambda"
  function_name    = "dataforge-company-ingestor"
  handler          = "ingest_company_careers.lambda_handler"
  lambda_role_arn  = module.iam.lambda_role_arn
  lambda_role_name = module.iam.lambda_role_name
  source_dir       = "../src"
  layers           = ["arn:aws:lambda:eu-central-1:336392948345:layer:AWSSDKPandas-Python311:12"]
  memory_size      = 1024 # Higher memory for parallel thread pool
  timeout          = 600  # 10 minutes — scrapes 200+ companies in parallel
  env_vars = {
    BRONZE_BUCKET                 = module.s3_bronze.bucket_id
    COMPANY_CAREERS_CONFIG_S3_URI = var.company_careers_config_s3_uri
    COMPANY_CAREERS_CONFIG_URL    = var.company_careers_config_url
    COMPANY_CAREERS_CONFIG_MODE   = var.company_careers_config_mode
  }
  bronze_bucket_arn = module.s3_bronze.arn
  enable_schedule   = true # Daily at 8AM UTC (same EventBridge schedule)
  enable_alerts     = true
  alert_email       = var.alert_email
}

# Silver Transformer (SCD Type 2 Logic)
module "transformer_lambda" {
  source           = "./modules/lambda"
  function_name    = "dataforge-transformer"
  handler          = "silver_transformer.lambda_handler"
  lambda_role_arn  = module.iam.lambda_role_arn
  lambda_role_name = module.iam.lambda_role_name
  source_dir       = "../src"
  memory_size      = 512
  timeout          = 300
  env_vars = {
    SILVER_PATH   = "s3://${module.s3_silver.bucket_id}/cleaned/jobs_history.parquet/"
    GOLD_BUCKET   = module.s3_gold.bucket_id
    BRONZE_BUCKET = module.s3_bronze.bucket_id
  }
  layers            = ["arn:aws:lambda:eu-central-1:336392948345:layer:AWSSDKPandas-Python311:12"]
  bronze_bucket_arn = module.s3_bronze.arn
  enable_schedule   = true
  schedule_expression = "cron(30 7,12,16 * * ? *)"
  enable_alerts     = true
  alert_email       = var.alert_email
}

# Gold Generator (runs after Silver is updated)
module "gold_lambda" {
  source           = "./modules/lambda"
  function_name    = "dataforge-gold-generator"
  handler          = "gold_generator.lambda_handler"
  lambda_role_arn  = module.iam.lambda_role_arn
  lambda_role_name = module.iam.lambda_role_name
  source_dir       = "../src"
  memory_size      = 512
  timeout          = 300
  layers           = ["arn:aws:lambda:eu-central-1:336392948345:layer:AWSSDKPandas-Python311:12"]
  env_vars = {
    SILVER_PATH = "s3://${module.s3_silver.bucket_id}/cleaned/jobs_history.parquet/"
    GOLD_BUCKET = module.s3_gold.bucket_id
  }
  bronze_bucket_arn = module.s3_bronze.arn
  enable_alerts     = true
  alert_email       = var.alert_email
}

# --- 4. AUTOMATION & TRIGGERS ---


# Silver .parquet write → gold generator
resource "aws_s3_bucket_notification" "on_silver_upload" {
  bucket = module.s3_silver.bucket_id
  lambda_function {
    lambda_function_arn = module.gold_lambda.lambda_function_arn
    events              = ["s3:ObjectCreated:*"]
    filter_suffix       = ".parquet"
  }
  depends_on = [module.gold_lambda]
}

resource "aws_lambda_permission" "allow_silver_s3" {
  statement_id  = "AllowSilverS3Invoke"
  action        = "lambda:InvokeFunction"
  function_name = module.gold_lambda.lambda_function_arn
  principal     = "s3.amazonaws.com"
  source_arn    = module.s3_silver.arn
}
