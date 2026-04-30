# 1. THE ROLE
resource "aws_iam_role" "lambda_role" {
  name = "dataforge-lambda-role-dev"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
    }]
  })
}

# 2. THE POLICY
resource "aws_iam_role_policy" "lambda_main_policy" {
  name = "dataforge-lambda-main-policy"
  role = aws_iam_role.lambda_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = ["s3:PutObject", "s3:GetObject", "s3:DeleteObject"]
        Resource = [
          "arn:aws:s3:::dataforge-bronze-dev-eu-central-1/*",
          "arn:aws:s3:::dataforge-silver-dev-eu-central-1/*",
          "arn:aws:s3:::dataforge-gold-dev-eu-central-1/*"
        ]
      },
      {
        Effect = "Allow"
        Action = ["s3:ListBucket"]
        Resource = [
          "arn:aws:s3:::dataforge-bronze-dev-eu-central-1",
          "arn:aws:s3:::dataforge-silver-dev-eu-central-1",
          "arn:aws:s3:::dataforge-gold-dev-eu-central-1"
        ]
      },
      {
        Effect   = "Allow"
        Action   = ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"]
        Resource = ["arn:aws:logs:eu-central-1:*:log-group:/aws/lambda/dataforge-*"]
      },
      {
        Effect   = "Allow"
        Action   = ["ssm:GetParameter"]
        Resource = "arn:aws:ssm:eu-central-1:*:parameter/dataforge*"
      }
    ]
  })
}