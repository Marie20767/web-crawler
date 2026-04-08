# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Architecture

Distributed web crawler with 3 Go microservices communicating via Kafka, storing raw HTML and parsed text in AWS S3:

```
Initialiser → Kafka (init topic) → Crawler → AWS S3
                                        ↓
                                Kafka (parser topic) → Parser (planned)
```

- **Initialiser** (`services/initialiser/`): Produces seed URLs to Kafka. One-shot execution.
- **Crawler** (`services/crawler/`): Consumes URLs, fetches HTML (2MB limit, 30s timeout), stores in S3, publishes `{ url: s3Link }` to parser topic.
- **Parser** (`services/parser/`): Fetches raw HTML from S3, parses text and URLs, stores parsed text in S3 and publishes new URLs to init topic 
- **Kafka** (`infra/kafka/`): Apache Kafka 4.2.0. Topics (`init`, `crawler-dlq`, `parser`, `parser-dlq`) are created via a docker-init container on startup.
- **Infra** (`infra/terraform/`): Terraform for AWS S3 buckets. Requires `aws sso login --profile terraform`.

Both services share the same package layout: `main.go`, `config/config.go`, and a domain package (`consumer/` or producer logic).

Go workspaces are set up with shared config and `go.mod` files under `/shared`.

## Commands

All commands are run from the relevant service directory (`services/initialiser/` | `services/crawler/` | `services/parser`).

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

## Session Start Checklist

Check if `.claude/REFLECTION_NEEDED.md` exists.

If it has content beyond the title `Reflection Notes`, update CLAUDE.md with any relevant architectural facts or decisions noted, then delete the file.
If the file is empty or missing, continue normally.

## End-of-Session Habits

- at the end of any session that involved significant debugging, new patterns, architectural decisions, or workflow friction, run `/reflection` before closing. If unsure whether the session qualifies, run it anyway.

- after any session that touches files under `services/` — whether modifying, fixing, or reviewing — run `make lint` and `make build` from the affected service directory. Fix any errors before finishing.

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