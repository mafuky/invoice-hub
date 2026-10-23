.PHONY: test lint proto

test:
	go test ./...

lint:
	golangci-lint run ./...

proto:
	buf generate
