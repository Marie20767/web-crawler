## Distributed Web Crawler

A distributed web crawler built with two Go microservices communicating via Apache Kafka, storing crawled HTML in AWS S3.

### Stack

- Go
- Apache Kafka
- AWS S3 (via Terraform)
- Docker

### Architecture

![high-level system architecture](apparchitecture.png)

### Repo Structure

This repository uses a multi-module structure with a Go workspace.

`/shared/go.mod` - reusable library code (e.g. config, common libraries, etc.)

`/services/crawler/go.mod`

`/services/initialiser/go.mod`

`go.work` - workspace definition (links modules together locally)

Each module manages its own dependencies independently.

### Development

#### Prerequisites

The crawler requires AWS SSO authentication before running:
```bash
aws sso login --profile terraform
```

#### Run full stack (Kafka + both services)
```bash
make up
```

#### Run services individually

Kafka must be running first:
```bash
cd infra/kafka && make up
# Kafka UI: http://localhost:8080
```

Then from `services/initialiser/` or `services/crawler/`:
```bash
make run
```

#### Lint
```bash
make lint        # install + run
make lint/fix    # auto-fix
```

#### Build (Linux x86_64 binary)
```bash
make build
```

### Infrastructure

Terraform config is in `infra/terraform/`. Requires `aws sso login --profile terraform` before applying.

### CI/CD

GitHub Actions workflows run on PRs to `master` (build + lint) and pushes to `master` (release to Docker Hub).
