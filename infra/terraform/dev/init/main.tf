terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
  required_version = ">= 1.6.0"
}

provider "aws" {
  region  = "ap-southeast-2"
  profile = "root"
}

# -------------------------------------------------------
# IAM sso user
# -------------------------------------------------------

data "aws_ssoadmin_instances" "main" {}

resource "aws_ssoadmin_permission_set" "terraform_infra" {
  name             = "TerraformInfraAdmin"
  description      = "Permission set for Terraform infrastructure management"
  instance_arn     = tolist(data.aws_ssoadmin_instances.main.arns)[0]
  session_duration = "PT4H"  # 4 hour sessions
}

resource "aws_ssoadmin_permission_set_inline_policy" "terraform_s3_scoped" {
  instance_arn       = tolist(data.aws_ssoadmin_instances.main.arns)[0]
  permission_set_arn = aws_ssoadmin_permission_set.terraform_infra.arn
  inline_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["s3:*"]
      Resource = [
        "arn:aws:s3:::my-web-crawler-data-dev",
        "arn:aws:s3:::my-web-crawler-data-dev/*"
      ]
    }]
  })
}

resource "aws_iam_policy" "terraform_iam_management" {
  name        = "TerraformIAMManagement"
  description = "Allows Terraform to manage IAM users, policies, and roles"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "IAMUserManagement"
        Effect = "Allow"
        Action = [
          "iam:CreateUser",
          "iam:DeleteUser",
          "iam:GetUser",
          "iam:ListUserTags",
          "iam:TagUser",
          "iam:UntagUser"
        ]
        Resource = "arn:aws:iam::*:user/web-crawler-*"
      },
      {
        Sid    = "IAMPolicyManagement"
        Effect = "Allow"
        Action = [
          "iam:ListPolicies", 
          "iam:CreatePolicy",
          "iam:DeletePolicy",
          "iam:GetPolicy",
          "iam:GetPolicyVersion",
          "iam:ListPolicyVersions",
          "iam:CreatePolicyVersion",
          "iam:DeletePolicyVersion",
          "iam:SetDefaultPolicyVersion",
          "iam:ListEntitiesForPolicy"
        ]
        Resource = "arn:aws:iam::${var.aws_account_id}:policy/web_crawler_*"
      },
      {
        Sid    = "IAMPolicyAttachment"
        Effect = "Allow"
        Action = [
          "iam:AttachUserPolicy",
          "iam:DetachUserPolicy",
          "iam:ListAttachedUserPolicies"
        ]
        Resource = "arn:aws:iam::*:user/web-crawler-*"
      }
    ]
  })
}

# Assign the permission set to your SSO user in your AWS account
resource "aws_ssoadmin_account_assignment" "terraform_runner" {
  instance_arn       = tolist(data.aws_ssoadmin_instances.main.arns)[0]
  permission_set_arn = aws_ssoadmin_permission_set.terraform_infra.arn
  principal_id       = var.sso_user_id
  principal_type     = "USER"
  target_id          = var.aws_account_id
  target_type        = "AWS_ACCOUNT"
}

# -------------------------------------------------------
# IAM user for k8s workload
# -------------------------------------------------------

resource "aws_iam_user" "web_crawler" {
  name = "web-crawler-k8s"
  tags = {
    Purpose = "Web crawler k8s workload"
  }
}

resource "aws_iam_access_key" "web_crawler" {
  user = aws_iam_user.web_crawler.name
}

resource "aws_iam_policy" "web_crawler_s3" {
  name        = "web_crawler_s3_access"
  description = "Allows web crawler to put and fetch objects from s3"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "S3PutObject"
        Effect = "Allow"
        Action = [
          "s3:PutObject",
          "s3:GetObject",
          "s3:ListBucket"
        ]
        Resource = [
          "arn:aws:s3:::my-web-crawler-data-dev",
          "arn:aws:s3:::my-web-crawler-data-dev/*"
        ]
      }
    ]
  })
}

resource "aws_iam_user_policy_attachment" "web_crawler_s3" {
  user       = aws_iam_user.web_crawler.name
  policy_arn = aws_iam_policy.web_crawler_s3.arn
}