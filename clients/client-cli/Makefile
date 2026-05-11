BIN_NAME      := queenctl
PKG           := github.com/smartpricing/queen/clients/client-cli
CMD_PKG       := $(PKG)/cmd
BUILD_DIR     := bin
INSTALL_DIR   := $(or $(GOBIN),$(HOME)/go/bin)

VERSION       := $(shell git describe --tags --abbrev=0 --match 'clients/client-cli/v*' 2>/dev/null | sed 's|clients/client-cli/||')
ifeq ($(strip $(VERSION)),)
VERSION := dev
endif
COMMIT        := $(shell git rev-parse --short HEAD 2>/dev/null || echo none)
DATE          := $(shell date -u +%Y-%m-%dT%H:%M:%SZ)

LDFLAGS := -s -w \
  -X '$(CMD_PKG).BuildVersion=$(VERSION)' \
  -X '$(CMD_PKG).BuildCommit=$(COMMIT)' \
  -X '$(CMD_PKG).BuildDate=$(DATE)'

GO            ?= go
GOFLAGS       :=

.PHONY: help
help: ## Show this help.
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z0-9_.-]+:.*?## / {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

.PHONY: build
build: ## Build queenctl into ./bin/queenctl.
	@mkdir -p $(BUILD_DIR)
	$(GO) build $(GOFLAGS) -ldflags "$(LDFLAGS)" -o $(BUILD_DIR)/$(BIN_NAME) .

.PHONY: install
install: ## Install queenctl into $GOBIN (or $HOME/go/bin).
	$(GO) install $(GOFLAGS) -ldflags "$(LDFLAGS)" .

.PHONY: test
test: ## Run unit tests (skips end-to-end).
	$(GO) test ./...

.PHONY: e2e
e2e: build ## Run the full end-to-end suite. Requires a running broker. \
            ## Honored env vars: QUEEN_SERVER, PG_HOST/PORT/USER/PASSWORD/DB, \
            ## QUEEN_RETENTION_INTERVAL_MS, QUEEN_LOAD_TOTAL.
	QUEEN_E2E=1 \
	  QUEEN_SERVER=$${QUEEN_SERVER:-http://localhost:6632} \
	  $(GO) test -v ./tests/... -timeout 10m

.PHONY: e2e-short
e2e-short: build ## Run only the fast slice of the e2e suite (queue + push + pop + tx).
	QUEEN_E2E=1 \
	  QUEEN_SERVER=$${QUEEN_SERVER:-http://localhost:6632} \
	  $(GO) test -v ./tests/... -timeout 5m -run 'TestQueue|TestPush|TestPop|TestTx'

.PHONY: vet
vet: ## Run go vet.
	$(GO) vet ./...

.PHONY: tidy
tidy: ## Run go mod tidy.
	$(GO) mod tidy

.PHONY: clean
clean: ## Remove build artifacts.
	rm -rf $(BUILD_DIR)

.PHONY: completion
completion: build ## Regenerate shell completion scripts.
	@mkdir -p completions
	./$(BUILD_DIR)/$(BIN_NAME) completion bash       > completions/$(BIN_NAME).bash
	./$(BUILD_DIR)/$(BIN_NAME) completion zsh        > completions/$(BIN_NAME).zsh
	./$(BUILD_DIR)/$(BIN_NAME) completion fish       > completions/$(BIN_NAME).fish
	./$(BUILD_DIR)/$(BIN_NAME) completion powershell > completions/$(BIN_NAME).ps1

.PHONY: release
release: ## Run goreleaser locally (snapshot).
	goreleaser release --snapshot --clean

.PHONY: release-dry
release-dry: ## Run goreleaser in dry-run mode.
	goreleaser check
