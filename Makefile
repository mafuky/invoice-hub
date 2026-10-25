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

.PHONY: dev demo reconcile crash-demo test-integration test-lesson

SCENARIO ?= scenarios/chaos.yaml
export SCENARIO
N ?= 100
TIMEOUT ?= 10m

dev:
	go run github.com/mattn/goreman@v0.3.16 start

demo:
	go run ./cmd/demo -n $(N) -scenario "$(SCENARIO)" -timeout "$(TIMEOUT)"

reconcile:
	go run ./cmd/reconcile -since "$(SINCE)"

crash-demo:
	./scripts/crash-demo.sh -n $(N) -scenario "$(SCENARIO)" -timeout "$(TIMEOUT)"

test-integration:
	go test -p 1 -tags 'lesson integration' ./...

# N -> packages (only the selected lesson test files run).
# 1 -> internal/invoice/domain internal/invoice/store
# 2 -> internal/invoice/app internal/platform/outbox
# 3 -> internal/platform/consume internal/platform/mq
# 4 -> internal/provider/agent
# 5 -> internal/provider/callback
# 6 -> internal/delivery/agent internal/delivery/scan
# 7 -> internal/settlement/domain internal/settlement/app
# 8 -> internal/settlement/verify
# 9 -> internal/invoice/app
LESSON_PACKAGES_1 := internal/invoice/domain internal/invoice/store
LESSON_PACKAGES_2 := internal/invoice/app internal/platform/outbox
LESSON_PACKAGES_3 := internal/platform/consume internal/platform/mq
LESSON_PACKAGES_4 := internal/provider/agent
LESSON_PACKAGES_5 := internal/provider/callback
LESSON_PACKAGES_6 := internal/delivery/agent internal/delivery/scan
LESSON_PACKAGES_7 := internal/settlement/domain internal/settlement/app
LESSON_PACKAGES_8 := internal/settlement/verify
LESSON_PACKAGES_9 := internal/invoice/app
LESSON_TESTS_1 := internal/invoice/domain/canto_test.go internal/invoice/store/transition_test.go
LESSON_TESTS_2 := internal/invoice/app/apply_test.go internal/platform/outbox/relay_test.go
LESSON_TESTS_3 := internal/platform/consume/idempotent_test.go internal/platform/mq/retry_test.go
LESSON_TESTS_4 := internal/provider/agent/provider_test.go internal/provider/agent/provider_integration_test.go
LESSON_TESTS_5 := internal/provider/callback/handle_test.go
LESSON_TESTS_6 := internal/delivery/agent/deliver_test.go internal/delivery/agent/deliver_integration_test.go internal/delivery/scan/recover_test.go
LESSON_TESTS_7 := internal/settlement/domain/canto_test.go internal/settlement/app/settle_test.go
LESSON_TESTS_8 := internal/settlement/verify/rules_test.go
LESSON_TESTS_9 := internal/invoice/app/commands_test.go internal/invoice/app/timeout_test.go

# File lists keep Apply (lesson 2) separate from commands/timeout (lesson 9),
# and exclude ordinary integration tests in packages that also contain lessons.
test-lesson:
	@test -n "$(LESSON_PACKAGES_$(N))" || { echo 'usage: make test-lesson N=1..9' >&2; exit 2; }
	@set -eu; for pkg in $(LESSON_PACKAGES_$(N)); do \
		set --; \
		for file in $(LESSON_TESTS_$(N)); do \
			case "$$file" in "$$pkg/"*) set -- "$$@" "$${file##*/}" ;; esac; \
		done; \
		(cd "$$pkg"; sources=$$(go list -tags 'lesson integration' -f '{{join .GoFiles " "}}' .); \
			go test -p 1 -tags 'lesson integration' -run . $$sources "$$@"); \
	done
