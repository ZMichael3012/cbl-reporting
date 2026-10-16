.PHONY: build test test-integration cover vet lint fmt up down

build:
	go build ./...

test:
	go test ./...

# test-integration runs integration tests (testcontainers: Kafka, ClickHouse).
test-integration:
	go test -tags integration -count=1 ./internal/...

# cover computes the combined unit+integration coverage over internal packages
# (generated code, main and migrations are excluded; the CBL-001 threshold is 80%);
# the cover.out profile is not committed (.gitignore).
cover:
	go test -tags integration -count=1 -coverpkg=./internal/... -coverprofile=cover.out ./internal/...
	go tool cover -func=cover.out

# E2E: the run is performed by an agent per test/e2e/E2E.md (platform-independent,
# artifacts in test/e2e/.runs/<UTC mark>/); there are no PowerShell run scripts.

vet:
	go vet ./...
	go vet -tags "integration e2e" ./...

lint:
	docker run --rm -v "$(CURDIR)/..":/src -w /src/$(notdir $(CURDIR)) golangci/golangci-lint:latest-alpine golangci-lint run ./...

fmt:
	gofmt -l .

up:
	docker compose -f deploy/docker-compose.yml --profile app up -d --build

down:
	docker compose -f deploy/docker-compose.yml down -v
