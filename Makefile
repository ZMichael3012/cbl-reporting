.PHONY: build test test-integration cover e2e vet lint fmt up down

build:
	go build ./...

test:
	go test ./...

# test-integration гоняет integration-тесты (testcontainers: Kafka, ClickHouse).
test-integration:
	go test -tags integration -count=1 ./internal/...

# cover считает объединенное покрытие unit+integration по internal-пакетам
# (generated-код, main и миграции не входят; порог CBL-001 - 80%);
# профиль cover.out не коммитится (.gitignore).
cover:
	go test -tags integration -count=1 -coverpkg=./internal/... -coverprofile=cover.out ./internal/...
	go tool cover -func=cover.out

# e2e поднимает compose-стенд сервиса (Kafka + ClickHouse + ledger-stub),
# применяет миграции и реконсиляцию через сервис и прогоняет сценарии:
# конвейер событий до fact_* (11.2) и словарь/backfill/сверка (11.3).
e2e:
	powershell -NoProfile -File test/e2e/ingestion.ps1
	powershell -NoProfile -File test/e2e/vocabulary.ps1

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
