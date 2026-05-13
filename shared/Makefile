PROJECT_ROOT := $(shell git rev-parse --show-toplevel)/

lint: lint/install lint/run

lint/install:
	curl -sSfL https://golangci-lint.run/install.sh | sh -s -- v2.12.2

lint/run:
	bin/golangci-lint run --config $(PROJECT_ROOT).golangci.yml

lint/fix:
	bin/golangci-lint run --config $(PROJECT_ROOT).golangci.yml --fix