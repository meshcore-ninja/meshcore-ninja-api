# MeshCore Ninja API build helpers.

GO ?= go
API_BIN ?= bin/meshcore-ninja-api
API_CONFIG ?= config.toml
API_VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)

.DEFAULT_GOAL := help

.PHONY: help
help: ## Show this help
	@grep -hE '^[a-zA-Z0-9_-]+:.*?## ' $(MAKEFILE_LIST) \
		| sort \
		| awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

.PHONY: build
build: ## Compile the API
	$(GO) build -ldflags "-X main.version=$(API_VERSION)" -o $(API_BIN) .

.PHONY: run
run: ## Run the API with the TOML config file
	$(GO) run . --config $(API_CONFIG)

.PHONY: test
test: ## Run Go tests
	$(GO) test ./...

.PHONY: vet
vet: ## Run go vet
	$(GO) vet ./...

.PHONY: fmt
fmt: ## Format Go code
	$(GO) fmt ./...

.PHONY: tidy
tidy: ## Tidy the Go module
	$(GO) mod tidy

.PHONY: clean
clean: ## Remove build artifacts
	rm -rf bin
