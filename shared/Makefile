PROJECT_ROOT := $(shell git rev-parse --show-toplevel)/

lint: lint/install lint/run

lint/install:
	curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s v2.5.0

lint/run:
	bin/golangci-lint run --config $(PROJECT_ROOT).golangci.yml

lint/fix:
	bin/golangci-lint run --config $(PROJECT_ROOT).golangci.yml --fix