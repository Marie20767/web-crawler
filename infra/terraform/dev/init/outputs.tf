output "sso_identity_store_id" {
  value       = tolist(data.aws_ssoadmin_instances.main.identity_store_ids)[0]
  description = "Identity store ID (not the start URL — construct start URL separately)"
}

output "web_crawler_access_key_id" {
  value     = aws_iam_access_key.web_crawler.id
  sensitive = true
}

output "web_crawler_secret_access_key" {
  value     = aws_iam_access_key.web_crawler.secret
  sensitive = true
}