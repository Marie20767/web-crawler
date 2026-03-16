GIT_HASH := $(shell git rev-parse --short HEAD)
DOCKER_USER := marie20767
IMAGE_NAME := go-url-producer

dep:
	go mod download

run:
	go run main.go

lint: lint/install lint/run

lint/install:
	curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s v2.5.0

lint/run:
	bin/golangci-lint run --config .golangci.yml

lint/fix:
	bin/golangci-lint run --config .golangci.yml --fix

build:
	CGO_ENABLED=0 \
	GOOS=linux \
	GOARCH=amd64 \
	go build -o go-url-producer .

docker/local-build:
	DOCKER_BUILDKIT=1 docker buildx build \
	-t $(DOCKER_USER)/$(IMAGE_NAME):local .

# For CI caching
CACHE_FROM ?=
CACHE_TO ?=

docker/ci-build:
	DOCKER_BUILDKIT=1 docker buildx build \
	$(CACHE_FROM) \
	$(CACHE_TO) \
	--platform linux/amd64 \
	-t $(DOCKER_USER)/$(IMAGE_NAME):latest \
	-t $(DOCKER_USER)/$(IMAGE_NAME):$(GIT_HASH) .

docker/push:
	docker push --all-tags $(DOCKER_USER)/$(IMAGE_NAME)

docker/build-and-push:
	DOCKER_BUILDKIT=1 docker buildx build \
	--platform linux/amd64 \
	--push \
	-t $(DOCKER_USER)/$(IMAGE_NAME):latest \
	-t $(DOCKER_USER)/$(IMAGE_NAME):$(GIT_HASH) .