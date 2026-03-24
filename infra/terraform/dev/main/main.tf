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
  profile = "terraform"
}

resource "aws_s3_bucket" "crawler_data" {
  bucket = "my-web-crawler-data-dev"
}

resource "aws_s3_bucket_public_access_block" "crawler_data" {
  bucket = aws_s3_bucket.crawler_data.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}