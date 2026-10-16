# cbl-reporting

Отчетность из ClickHouse: конвейер событий Kafka (Kafka engine + materialized view),
хранилище `reporting.fact_*`, словарь счетов `dim_accounts` и сверка с Ledger.

Роль сервиса и контракты: `AGENTS.md`. Глобальные правила платформы: `../../AGENTS.md`,
`../../.specs/CBL-000-system-description.md`, `../../.specs/CBL-003-contracts.md`.

## Команды

```powershell
make build             # сборка
make test              # unit-тесты
make test-integration  # integration (testcontainers: Kafka, ClickHouse)
make cover             # объединенное покрытие unit+integration
make e2e               # compose-стенд: конвейер (11.2) и словарь/сверка (11.3)
make vet               # go vet
make lint              # golangci-lint в Docker
make up                # сервисный стенд: kafka, schema-registry, clickhouse, ledger-stub, reporting
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

## Словарь и сверка (11.3)

`dim_accounts` наполняется воркером: счета мерчантов из `LedgerService.ListMerchantAccounts`,
системные (`clearing`, `fee`, `suspense`) - деривацией `account_id` по плану счетов
(`CBL-003`, раздел 9; golden-векторы). Новые строки леджера обогащаются на входе
(JOIN в MV), история - мутациями backfill. Воркер сверки периодически сравнивает
окно проводок (`fact_ledger_entries` FINAL) с `ListEntries` Ledger и отражает
расхождения (`missing_in_reporting`, `extra_in_reporting`, `amount_mismatch`) в
логах и счетчиках `/metrics`: это контроль достоверности конвейера, не денежные
решения (T-09) и не `recon.alerts` (зона Reconciler).

## Отчеты Query (11.4)

gRPC `billing.reporting.v1.ReportingService.Query` (:9110) отдает страницы закрытого
каталога: `payments`, `payouts`, `ledger_entries` (мерчантский фильтр по
денормализации 11.3) и `turnover` (`group_by`: `asset`, `day_asset`). Курсор - keyset
по ключам сортировки отчета, лимит до 100 строк, деньги - канонические десятичные
строки. Прокси мерчант API - `GET /v1/reports/{report_type}` в `cbl-billing-api`
(мерчант из токена).
