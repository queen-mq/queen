# CM-BENCH build.
#
# GOWORK=off everywhere: this harness is a standalone module that gets shipped
# to a bare VM, and the repo's go.work (clients only) must not leak into it.
# Same convention as benchmark-queen/*/goload.

GO      ?= go
GOFLAGS ?=
export GOWORK=off

BIN     := cmbench
PKG     := ./cmd/cmbench

.PHONY: all build test vet fmt lint linux clean control help

all: vet test build

build: ## build the harness for this machine
	$(GO) build $(GOFLAGS) -o $(BIN) $(PKG)

linux: ## cross-build a static amd64 binary to scp to the loader VM
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 $(GO) build $(GOFLAGS) \
		-ldflags='-s -w' -o $(BIN)-linux-amd64 $(PKG)

linux-arm: ## cross-build a static arm64 binary
	CGO_ENABLED=0 GOOS=linux GOARCH=arm64 $(GO) build $(GOFLAGS) \
		-ldflags='-s -w' -o $(BIN)-linux-arm64 $(PKG)

test: ## unit tests: verifier semantics, workload invariants, harness control
	$(GO) test ./internal/...

test-short: ## skip the timed end-to-end runs
	$(GO) test -short ./internal/...

vet:
	$(GO) vet ./...

fmt:
	gofmt -w .

control: build ## the experiment's control: harness against a perfect broker
	./$(BIN) -system mem -rate 500 -properties 50 -duration 20 -ramp 0 -drain 30 \
		-logdir ./cmlogs-control

clean:
	rm -f $(BIN) $(BIN)-linux-amd64 $(BIN)-linux-arm64
	rm -rf ./cmlogs ./cmlogs-control

help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'
