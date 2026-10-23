.PHONY: test lint proto

test:
	go test ./...

lint:
	golangci-lint run ./...

proto:
	buf generate

# Infrastructure targets (up, down, ps, tunnel, tunnel-stop, dev-check) live in infra.mk.
-include infra.mk
