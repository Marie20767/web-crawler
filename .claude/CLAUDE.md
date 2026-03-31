# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Architecture

Distributed web crawler with two Go microservices communicating via Kafka, storing output in AWS S3:

```
Initialiser → Kafka (url topic) → Crawler → AWS S3
```

- **Initialiser** (`services/initialiser/`): Produces seed URLs to Kafka. One-shot execution.
- **Crawler** (`services/crawler/`): Consumes URLs, fetches HTML (2MB limit, 30s timeout), stores in S3.
- **Kafka** (`infra/kafka/`): Apache Kafka 4.2.0, auto-creates the `url` topic on startup.
- **Infra** (`infra/terraform/`): Terraform for AWS S3 buckets. Requires `aws sso login --profile terraform`.

Both services share the same package layout: `main.go`, `config/config.go`, and a domain package (`consumer/` or producer logic).

## Commands

All commands are run from the relevant service directory (`services/crawler/` or `services/initialiser/`).

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

Check if .claude/REFLECTION_NEEDED.md exists.

If it does, read its contents and treat them as additional context for this session — patterns, friction, or decisions made outside Claude that should inform your understanding of the codebase.
If the file has content beyond the title `Reflection Notes` and the session warrants it (significant work noted),
incorporate it when running /reflection. After reading, delete the file so it doesn't accumulate stale entries.
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