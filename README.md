# cbl-reporting

Reporting from ClickHouse: the Kafka event pipeline (Kafka engine + materialized view),
the `reporting.fact_*` storage, the `dim_accounts` account vocabulary and reconciliation
against the Ledger.

The service role and contracts: `AGENTS.md`. Platform-wide rules: `../../AGENTS.md`,
`../../.specs/CBL-000-system-description.md`, `../../.specs/CBL-003-contracts.md`.

## Commands

```powershell
make build             # build
make test              # unit tests
make test-integration  # integration (testcontainers: Kafka, ClickHouse)
make cover             # merged unit+integration coverage
make vet               # go vet
make lint              # golangci-lint in Docker
make up                # service stand: kafka, schema-registry, clickhouse, ledger-stub, reporting
make down
```

E2E: the run is performed by an agent following `test/e2e/E2E.md` (the ingestion,
vocabulary, reports scenarios); artifacts go to `test/e2e/.runs/<UTC mark>/`, auxiliary
files to `test/e2e/.additionals/`.

## Pipeline

The topics `payment.events`, `ledger.events`, `payout.events`, `recon.alerts` are read
by the ClickHouse Kafka engine, materialized views write into `fact_*` (ReplacingMergeTree
deduplication by `event_id`/`alert_id`). The decoding wire schema is
`deploy/format_schemas/events.proto` (a flattening of `cbl-protos@cf-3.4`), mounted into
the ClickHouse `format_schema_path`. Schema migrations live in `migrations/` (goose,
clickhouse dialect) and are applied by the service at startup; Kafka tables and MVs are
reconciled from `KAFKA_BROKERS`/`SCHEMA_PATH`.

## Vocabulary and reconciliation (11.3)

`dim_accounts` is populated by a worker: merchant accounts from
`LedgerService.ListMerchantAccounts`, system accounts (`clearing`, `fee`, `suspense`) by
`account_id` derivation from the account plan (`CBL-003`, section 9; golden vectors). New
ledger rows are enriched on input (a JOIN in the MV), the history is processed by backfill
mutations. The reconciliation worker periodically compares a window of entries
(`fact_ledger_entries` FINAL) with the Ledger `ListEntries` and reflects discrepancies
(`missing_in_reporting`, `extra_in_reporting`, `amount_mismatch`) in logs and `/metrics`
counters: this is pipeline integrity control, not money decisions (T-09) and not
`recon.alerts` (the Reconciler domain).

## Query reports (11.4)

gRPC `billing.reporting.v1.ReportingService.Query` (:9110) serves pages of the closed
catalog: `payments`, `payouts`, `ledger_entries` (the merchant filter based on the 11.3
denormalization) and `turnover` (`group_by`: `asset`, `day_asset`). The cursor is keyset
over the report sort keys, the limit is up to 100 rows, money is canonical decimal
strings. The merchant API proxy is `GET /v1/reports/{report_type}` in `cbl-billing-api`
(merchant from the token).
