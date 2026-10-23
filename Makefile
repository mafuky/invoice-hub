.PHONY: test lint proto install-proto-tools migrate

# protoc plugins are installed with go install into GOPATH/bin.
GOBIN_DIR := $(shell go env GOPATH)/bin

test:
	go test ./...

lint:
	golangci-lint run ./...

# Needs the plugins from install-proto-tools on GOPATH/bin.
proto:
	cd proto && PATH="$(GOBIN_DIR):$$PATH" go run github.com/bufbuild/buf/cmd/buf@v1.73.0 generate

install-proto-tools:
	go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.12
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.6.2

# Infrastructure targets (up, down, ps, tunnel, tunnel-stop, dev-check) live in infra.mk.
-include infra.mk

# Needs the tunnel (make tunnel) and a filled .env.
migrate:
	go run ./cmd/migrate
