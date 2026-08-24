# GitHub Actions OIDC federation — replaces long-lived AWS access keys in repo secrets.
# Workflows assume this role via aws-actions/configure-aws-credentials with `id-token: write`.
# Free: IAM and STS have no cost.

locals {
  github_repo = "ritesh8303/dataforge"
}

resource "aws_iam_openid_connect_provider" "github" {
  url            = "https://token.actions.githubusercontent.com"
  client_id_list = ["sts.amazonaws.com"]
  # AWS validates GitHub's OIDC cert against trusted root CAs; thumbprints are
  # still required by the API but effectively ignored for this provider.
  thumbprint_list = [
    "6938fd4d98bab03faadb97b34396831e3780aea1",
    "1c58a3a8518e8759bf075b76b750d4f2df264fcd",
  ]
}

resource "aws_iam_role" "github_actions" {
  name = "dataforge-github-actions-dev"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Federated = aws_iam_openid_connect_provider.github.arn
        }
        Action = "sts:AssumeRoleWithWebIdentity"
        Condition = {
          StringEquals = {
            "token.actions.githubusercontent.com:aud" = "sts.amazonaws.com"
          }
          StringLike = {
            "token.actions.githubusercontent.com:sub" = "repo:${local.github_repo}:*"
          }
        }
      }
    ]
  })
}

# Least privilege for the two workflows that touch AWS:
#  - eures_scraper.yml writes Bronze parquet
#  - publish_gold.yml downloads Gold CSVs
resource "aws_iam_role_policy" "github_actions" {
  name = "dataforge-github-actions-s3"
  role = aws_iam_role.github_actions.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "BronzeWrite"
        Effect = "Allow"
        Action = [
          "s3:PutObject",
          "s3:GetObject",
          "s3:AbortMultipartUpload",
        ]
        Resource = "${module.s3_bronze.arn}/*"
      },
      {
        Sid      = "GoldRead"
        Effect   = "Allow"
        Action   = ["s3:GetObject"]
        Resource = "${module.s3_gold.arn}/*"
      },
      {
        Sid    = "ListBuckets"
        Effect = "Allow"
        Action = ["s3:ListBucket"]
        Resource = [
          module.s3_bronze.arn,
          module.s3_gold.arn,
        ]
      },
    ]
  })
}

output "github_actions_role_arn" {
  description = "Set this as role-to-assume in GitHub workflows"
  value       = aws_iam_role.github_actions.arn
}
