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
# IAM Identity Center permission set for Terraform runner
# -------------------------------------------------------

data "aws_ssoadmin_instances" "main" {}

resource "aws_ssoadmin_permission_set" "terraform_infra" {
  name             = "TerraformInfraAdmin"
  description      = "Permission set for Terraform infrastructure management"
  instance_arn     = tolist(data.aws_ssoadmin_instances.main.arns)[0]
  session_duration = "PT4H"  # 4 hour sessions
}

resource "aws_ssoadmin_managed_policy_attachment" "terraform_s3" {
  instance_arn       = tolist(data.aws_ssoadmin_instances.main.arns)[0]
  permission_set_arn = aws_ssoadmin_permission_set.terraform_infra.arn
  managed_policy_arn = "arn:aws:iam::aws:policy/AmazonS3FullAccess"
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
        Resource = "arn:aws:iam::*:policy/web_crawler_*"
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