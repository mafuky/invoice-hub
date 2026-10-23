.PHONY: test lint proto

# protoc plugins are installed with go install into GOPATH/bin.
GOBIN_DIR := $(shell go env GOPATH)/bin

test:
	go test ./...

lint:
	golangci-lint run ./...

proto:
	cd proto && PATH="$(GOBIN_DIR):$$PATH" go run github.com/bufbuild/buf/cmd/buf@v1.73.0 generate

# Infrastructure targets (up, down, ps, tunnel, tunnel-stop, dev-check) live in infra.mk.
-include infra.mk
