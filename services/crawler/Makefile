GIT_HASH := $(shell git rev-parse --short HEAD)
DOCKER_USER := marie20767
IMAGE_NAME := url-crawler
PROJECT_ROOT := $(shell git rev-parse --show-toplevel)/

dep:
	go mod download

run:
	aws sso login --profile terraform
	go run main.go

lint: lint/install lint/run

lint/install:
	curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s v2.5.0

lint/run:
	bin/golangci-lint run --config $(PROJECT_ROOT).golangci.yml

lint/fix:
	bin/golangci-lint run --config $(PROJECT_ROOT).golangci.yml --fix

build:
	CGO_ENABLED=0 \
	GOOS=linux \
	GOARCH=amd64 \
	go build -o url-crawler .

docker/local-build:
	DOCKER_BUILDKIT=1 docker buildx build \
	-f docker/Dockerfile \
	-t $(DOCKER_USER)/$(IMAGE_NAME):local \
	../..

# For CI caching
CACHE_FROM ?=
CACHE_TO ?=

docker/ci-build:
	DOCKER_BUILDKIT=1 docker buildx build \
	$(CACHE_FROM) \
	$(CACHE_TO) \
	--platform linux/amd64 \
	-f docker/Dockerfile \
	-t $(DOCKER_USER)/$(IMAGE_NAME):latest \
	-t $(DOCKER_USER)/$(IMAGE_NAME):$(GIT_HASH) \
	../..

docker/push:
	docker push --all-tags $(DOCKER_USER)/$(IMAGE_NAME)

docker/build-and-push:
	DOCKER_BUILDKIT=1 docker buildx build \
	--platform linux/amd64 \
	--push \
	-f docker/Dockerfile \
	-t $(DOCKER_USER)/$(IMAGE_NAME):latest \
	-t $(DOCKER_USER)/$(IMAGE_NAME):$(GIT_HASH) \
	../..