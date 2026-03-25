output "sso_start_url" {
  value       = tolist(data.aws_ssoadmin_instances.main.identity_store_ids)[0]
  description = "Use this to configure your SSO profile"
}