# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Architecture

Distributed web crawler with 3 Go microservices communicating via Kafka, storing raw HTML and parsed text in AWS S3 and URL metadata in MongoDB.

Flow:

```
Initialiser → Kafka (url topic) → Crawler → AWS S3 (raw HTML)
                                        ↓
                              Kafka (parser topic) → Parser → AWS S3 (parsed text)
                                                         ↓
                                              Kafka (url topic)
```

- **Initialiser** (`services/initialiser/`): Produces seed URLs to Kafka. One-shot execution.
- **Crawler** (`services/crawler/`): Consumes URLs, fetches HTML, stores raw HTML in S3 and URL metadata in MongoDB, and produces to parser topic.
- **Parser** (`services/parser/`): Fetches raw HTML from S3, extracts text and URLs, stores parsed text in S3 and produces new URLs to url topic.
- **Kafka** (`infra/kafka/`): Runs only via Docker. Apache Kafka 4.2.0. Topics (`url`, `crawler-dlq`, `parser`, `parser-dlq`) are created via a kafka-init container on startup.
- **MongoDB** (`infra/mongodb/`): Runs only via Docker. The `url` collection is used for deduplication — checked before fetching HTML (crawler) and before producing to the url topic (parser). The `domain` collection checks crawling rules to ensure polite crawling.
- **Infra** (`infra/terraform/`): Terraform for AWS S3 buckets. Requires `aws sso login --profile terraform`.

All services share the same package layout: `main.go`, `config/config.go`, and a domain package (`consumer/` and `/producer` logic).

Go workspaces are set up with shared config and `go.mod` files under `/shared`.

## Commands

All commands are run from the relevant service directory (`services/initialiser/` | `services/crawler/` | `services/parser`) or shared directory (`shared/`)

```bash
# Run (crawler requires AWS SSO first: aws sso login --profile terraform)
make run

# Lint
make lint          # install + run
make lint/fix      # auto-fix

# Build (Linux x86_64 binary)
make build

# Docker
make docker/local-build
make docker/build-and-push
```

```bash
# Start full stack (Kafka + both services)
make up    # from repo root

# Kafka only
cd infra/kafka && make up
# Kafka UI: http://localhost:8080
```

There are currently no test files in this codebase.

## CI/CD

GitHub Actions workflows trigger on PRs to `master` (build + lint) and pushes to `master` (release to Docker Hub via `DOCKER_USERNAME` / `DOCKER_PASSWORD` secrets).

## Go
- Target Go 1.25+ — always use modern idioms and APIs.
- Use `wg.Go(func() { ... })` instead of `wg.Add(1)` + deferred `wg.Done()`.
- Prefer `sync/atomic` typed operations, `slices`, `maps`, and `cmp` standard library packages where applicable.
- Do not use deprecated pre-1.21 patterns (e.g. manual `sort.Slice` over `slices.SortFunc`).

## End-of-Session Habits

- at the end of any session that involved significant debugging, new patterns, architectural decisions, or workflow friction, run `/reflection` before closing. If unsure whether the session qualifies, run it anyway.

- after any session that touches files under `services/` — whether modifying, fixing, or reviewing — run `make lint` and `make build` from the affected service directory. Fix any errors before finishing. If the session touches files under `shared/`, run `make lint` from the shared directory and fix any errors before finishing.

## Important to note

This is a personal project, but it should be treated as production-grade.

- Priorities:

Follow production conventions and best practices.
Prefer correctness, reliability, and maintainability over shortcuts.
Keep configurations, structure, and workflows aligned with real-world production setups.

- Constraints:

Resources are limited (infrastructure, cost, time).
Choose solutions that are efficient and lightweight while still reflecting production patterns.

- Trade-offs:

When full production fidelity is too costly, choose the closest practical approximation.
Avoid unnecessary complexity that does not provide meaningful production realism.