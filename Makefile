APP_NAME := kafka-replay
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
GIT_COMMIT := $(shell git rev-parse --short HEAD 2>/dev/null || echo unknown)
BUILD_DATE := $(shell date -u +%Y-%m-%dT%H:%M:%SZ)
LDFLAGS := -ldflags "-X github.com/vnayakg/kafka-replay/cmd.Version=$(VERSION) \
	-X github.com/vnayakg/kafka-replay/cmd.GitCommit=$(GIT_COMMIT) \
	-X github.com/vnayakg/kafka-replay/cmd.BuildDate=$(BUILD_DATE)"

# Auto-detect Docker socket for Colima/Docker Desktop
DOCKER_HOST ?= $(shell docker context inspect --format '{{.Endpoints.docker.Host}}' 2>/dev/null)
export DOCKER_HOST
export TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE ?= /var/run/docker.sock

.PHONY: build test lint bench clean

build:
	go build $(LDFLAGS) -o bin/$(APP_NAME) .

test:
	go test ./... -v -count=1

test-integration:
	go test ./... -v -count=1 -tags=integration -timeout=5m

bench:
	go test -tags=integration -bench=. -benchmem -benchtime=10s -timeout=10m ./benchmarks/

lint:
	golangci-lint run ./...

clean:
	rm -rf bin/

all: lint test build
