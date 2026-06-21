MAKEFLAGS := --no-print-directory --silent

default: help

help:
	@echo "Please use 'make <target>' where <target> is one of"
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z\._-]+:.*?## / {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

t: test
test: ## Test the code
	go test ./... -timeout=60s -parallel=10 --cover

tr: test.report
test.report: ## Test the code with coverage report
	go test ./... --cover -timeout=300s -parallel=64 -coverprofile coverage.out
	gocovsh

fmt: ## Format and check the code
	@go mod tidy
	@gofumpt -l -w .
	@golangci-lint run --timeout 600s
	@go vet ./...
	@gosec ./...

tools: ## Install extra tools for development
	go install mvdan.cc/gofumpt@latest
	go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest
	go install github.com/orlangure/gocovsh@latest
	go install github.com/securego/gosec/v2/cmd/gosec@latest

sec: ## Check code security
	gosec ./...

lint: ## Lint the code locally
	golangci-lint run --timeout 600s
