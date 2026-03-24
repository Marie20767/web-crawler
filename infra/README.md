# Web Crawler Infra

Infrastructure for web crawler, including:
- Provisioning with terraform

## Terraform

#### Prerequisites:
- AWS CLI
- Terraform
- An AWS user setup with IAM privileges
- Enable IAM Identity Centre (AWS Management Console)
- SSO user created on (AWS Management Console → IAM Identity Center → Users)

#### To setup:
1. Configure a temporary root CLI profile:
```
aws configure --profile root
```

2. Run init Terraform using root profile:
```
cd terraform/prod|dev/init
terraform init
terraform apply -var="sso_user_id=AWS_SSO_USER_ID" -var="aws_account_id=AWS_ACCOUNT_ID"
```

3. Configure SSO profile:
```
aws configure sso --profile terraform
```

When prompted, add the following SSO start URL: 
```
https://{SSO_START_URL}.awsapps.com/start
```

4. Delete root access key on AWS Management Console

5. Run init Terraform using SSO profile:
```
aws sso login --profile terraform

cd terraform/prod|dev/main
terraform init
terraform apply
```
