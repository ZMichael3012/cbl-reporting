# cbl-reporting

Отчетность из ClickHouse: конвейер событий Kafka (Kafka engine + materialized view),
хранилище `reporting.fact_*`.

Роль сервиса и контракты: `AGENTS.md`. Глобальные правила платформы: `../../AGENTS.md`,
`../../.specs/CBL-000-system-description.md`, `../../.specs/CBL-003-contracts.md`.

## Команды

```powershell
make build             # сборка
make test              # unit-тесты
make test-integration  # integration (testcontainers: Kafka, ClickHouse)
make e2e               # compose-стенд сервиса: конвейер до fact_*
make vet               # go vet
make lint              # golangci-lint в Docker
make up                # сервисный стенд: kafka, schema-registry, clickhouse, reporting
make down
```

## Конвейер

Топики `payment.events`, `ledger.events`, `payout.events`, `recon.alerts` читаются
ClickHouse Kafka engine, materialized view пишут в `fact_*` (дедупликация
ReplacingMergeTree по `event_id`/`alert_id`). Wire-схема декодирования -
`deploy/format_schemas/events.proto` (уплощение `cbl-protos@cf-3.4`), монтируется в
`format_schema_path` ClickHouse. Миграции схемы - `migrations/` (goose, диалект
clickhouse), применяются сервисом при старте; Kafka-таблицы и MV реконсилируются из
`KAFKA_BROKERS`/`SCHEMA_PATH`.
