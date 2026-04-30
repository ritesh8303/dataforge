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

resource "aws_ssm_parameter" "ba_api_keys" {
  name        = "/dataforge/dev/ba_api_credentials"
  description = "BA API OAuth2 Credentials (Cost Optimized)"
  type        = "SecureString"
  value       = "{\"client_id\": \"YOUR_ID\", \"client_secret\": \"YOUR_SECRET\"}"
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
  env_vars = {
    BRONZE_BUCKET      = module.s3_bronze.bucket_id
    SSM_PARAMETER_NAME = aws_ssm_parameter.ba_api_keys.name
  }
  bronze_bucket_arn = module.s3_bronze.arn
  enable_schedule   = true
  alert_email       = "riteshjadhav8303@gmail.com"
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
  layers            = ["arn:aws:lambda:eu-central-1:336392948345:layer:AWSSDKPandas-Python311:12"]
  bronze_bucket_arn = module.s3_bronze.arn
  enable_schedule   = true
  alert_email       = "riteshjadhav8303@gmail.com"
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
  env_vars         = { SILVER_PATH = "s3://${module.s3_silver.bucket_id}/cleaned/jobs_history.parquet/" }
  layers           = ["arn:aws:lambda:eu-central-1:336392948345:layer:AWSSDKPandas-Python311:12"]
  bronze_bucket_arn = module.s3_bronze.arn
  enable_schedule   = true
  enable_alerts     = true
  alert_email       = "riteshjadhav8303@gmail.com"
}

# --- 4. AUTOMATION & TRIGGERS ---
resource "aws_s3_bucket_notification" "on_json_upload" {
  bucket = module.s3_bronze.bucket_id
  lambda_function {
    lambda_function_arn = module.transformer_lambda.lambda_function_arn
    events              = ["s3:ObjectCreated:*"]
    filter_suffix       = ".parquet"
  }
  depends_on = [module.transformer_lambda]
}