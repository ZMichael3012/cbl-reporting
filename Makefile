.PHONY: build test test-integration e2e vet lint fmt up down

build:
	go build ./...

test:
	go test ./...

# test-integration гоняет integration-тесты (testcontainers: Kafka, ClickHouse).
test-integration:
	go test -tags integration -count=1 ./internal/...

# e2e поднимает compose-стенд сервиса (Kafka + ClickHouse), применяет миграции и
# реконсиляцию через сервис и проверяет конвейер событий до таблиц fact_*.
e2e:
	powershell -NoProfile -File test/e2e/ingestion.ps1

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
