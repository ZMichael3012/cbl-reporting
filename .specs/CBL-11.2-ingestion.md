# CBL-11.2. Event ingestion into ClickHouse: Kafka engine, materialized view, service storage

| Field | Value |
| :-- | :-- |
| Epic | E11, `Reporting and ClickHouse` |
| Repository | `cbl-reporting` |
| Size | L |
| Wave and parallel group | W3-REPORT |
| Depends on | `CBL-11.1` (`fact_*` schemas), contracts `cbl-protos@cf-3.4`, T-10 |
| Contract version | `cbl-protos@cf-3.4` |
| Status | done, review closed |

## 1. Context and goal

`CBL-000`: ClickHouse reads events from Kafka via Kafka engine + materialized view (T-10), deduplication - ReplacingMergeTree (`CBL-003`, section 7). By the decision of 15.10.2026 ClickHouse is the storage of the `cbl-reporting` service, as PostgreSQL is for DB-backed services: schema and migrations in the service repository, a local instance in the service `deploy/docker-compose.yml`, integration tests on testcontainers (Kafka + ClickHouse), e2e on the service compose; the global compose keeps a shared stand instance for end-to-end scenarios (GE-7), and the `cbl-system` init from 11.1 remains a dev-bootstrap.

Task 11.2 fills `fact_*` from four topics, moves schema ownership into the service (goose migrations), and adds service configuration, integration tests and e2e.

## 2. Scope

In scope: migrations of `fact_*` in the service (goose, clickhouse dialect), Kafka engine tables and materialized views for four topics, the wire decoding schema, service configuration and main (`CLICKHOUSE_DSN`, `KAFKA_BROKERS`, healthz/readyz, metrics stub), testcontainers integration (Kafka + ClickHouse, real protobuf messages), service compose e2e, Dockerfile/compose/Makefile/AGENTS.

Out of scope: gRPC `Query` and the `/v1/reports` proxy (11.4), the `dim_accounts` dictionary and reconciliation (11.3), dashboards (11.5), retention/TTL and quotas (14.7), Kafka Connect (T-10: only if strict exactly-once is required), writing to Kafka by the service (no producer).

## 3. Functional requirements

FR-1. Service storage: `CLICKHOUSE_DSN` (clickhouse-go/v2, database/sql driver), `/readyz` pings ClickHouse; the service compose brings up `clickhouse` (like PostgreSQL for DB-backed services) and mounts the wire schema directory into `format_schema_path`.

FR-2. Migrations: the `migrations/` directory in goose format with the `clickhouse` dialect, applied at service startup (`goose_db_version`); forward only; the fact DDL repeats 11.1 (the schema owner from this task is the service; the `cbl-system` init remains a dev-bootstrap).

FR-3. Pipeline per topic (`payment.events`, `ledger.events`, `payout.events`, `recon.alerts`): the table `kafka_<fact>` (Kafka engine, `ProtobufSingle`) + the materialized view `<fact>_mv` writes into `fact_*`. Kafka tables are reconciled at startup: the expected definition of the pair (brokers, schema, topic, group, columns, select) is folded into a fingerprint and stored in the table `COMMENT`; on a fingerprint mismatch (including shrinking of the broker list - comparison of values, not substrings) the pair is recreated, and in a partial state (table without view) the view is restored. Reconciliation is for a single writer; when scaling replicas the update scheme is agreed separately (14.2/0.6).

FR-4. Decoding: the `deploy/format_schemas/events.proto` file - a wire-compatible flattening of the contracts (`cbl-protos@cf-3.4`, field numbers and types preserved, unused fields omitted; the decoder skips unknown wire fields). Mapping: `Money` -> `Tuple(minor_units Int64, asset String, decimals UInt32)`; `Timestamp` -> `Tuple(seconds Int64, nanos Int32)` with conversion to `DateTime64(3, 'UTC')` (seconds plus nanoseconds truncated to milliseconds); `LedgerEntry[]` -> `Array(Tuple(...))` with `ARRAY JOIN ... WITH ORDINALITY` (`entry_index` 0-based); `event_id`/`event_type` - from the envelope headers (reconstruction of `_headers` via `arrayFirst`, in 24.8 this is an array of tuples), `occurred_at` - from the body (authoritative by contract). `SCHEMA_PATH` - a path relative to `format_schema_path` of the ClickHouse server (an absolute path is forbidden by Kafka engine).

FR-5. Pipeline error policy: `kafka_handle_error_mode='stream'`, `kafka_skip_broken_messages=1000` (a corrupted message does not stop the partition); the ledger MV filters `transaction_id != ''`, `direction IN (1, 2)`, `length(entries) <= 255`; the MVs additionally drop rows with empty key fields, invalid `decimals` (> 18) and alert `severity`/`type` outside the frozen values.

FR-6. Idempotency/deduplication: the MVs insert rows as is, Kafka duplicate collapsing is provided by ReplacingMergeTree `fact_*` (11.1); re-running the service does not create configuration duplicates (CREATE IF NOT EXISTS, the pair is recreated only on fingerprint drift).

FR-7. main: `HTTP_ADDR` (:8110, healthz/readyz), `GRPC_ADDR` (:9110, gRPC health service for orchestrators; `ReportingService.Query` - 11.4), `METRICS_ADDR` (:9310, stub until 0.7), `MIGRATIONS_DIR`, `SCHEMA_PATH` (the wire schema file inside ClickHouse), graceful shutdown, `run()` with a non-zero code on an initialization error (including a listener failure).

FR-8. Tests: unit (configuration, reconciliation SQL and filters); integration (testcontainers Kafka + ClickHouse): migrations, idempotent reconciliation, the full flow over four topics with real protobuf (generation via `cbl-protos`), value checks (money, entry_index, time conversion), duplicate deduplication, a broken message does not stop the pipeline; compose e2e: up -> topic creation -> event producer -> rows in `fact_*` -> event repeat -> readyz; k6 is not applicable (no HTTP load) - 14.2.

## 4. Contracts

- Kafka (consumption via the ClickHouse Kafka engine; the service does not run its own consumer): topics and keys per `CBL-003`, section 6; envelope headers - `event_id`, `event_type`, `occurred_at`, `producer`, `traceparent`, `schema_version` (`cbl-kit/outbox`); groups `cbl-reporting-<fact>`; at-least-once.
- ClickHouse: `reporting.fact_*` (11.1), `reporting.kafka_*` (Kafka engine), `reporting.<fact>_mv`; database `reporting`.
- gRPC: the health service (:9110); `ReportingService` - 11.4.
- REST: `/healthz`, `/readyz` (administrative).

## 5. Data model and migrations

Migration `00001_fact_tables.sql` - DDL of the four `fact_*` (identical to `CBL-11.1`). Runtime reconciliation (not a migration, documented trade-off T-RPT-01): for example

```sql
CREATE TABLE IF NOT EXISTS reporting.kafka_payment_events (
  payment_id String, merchant_id String,
  expected_amount Tuple(minor_units Int64, asset String, decimals UInt32),
  received_amount Tuple(minor_units Int64, asset String, decimals UInt32),
  fee Tuple(minor_units Int64, asset String, decimals UInt32),
  quote_id String, rate String, txid String, state_version UInt64,
  occurred_at Tuple(seconds Int64, nanos Int32))
ENGINE = Kafka
SETTINGS kafka_broker_list = '...', kafka_topic_list = 'payment.events',
  kafka_group_name = 'cbl-reporting-payment', kafka_format = 'ProtobufSingle',
  format_schema = 'events.proto:PaymentRow', kafka_handle_error_mode = 'stream',
  kafka_skip_broken_messages = 1000, kafka_num_consumers = 1
COMMENT 'cbl-pipeline:<fingerprint of DDL and configuration>';
```

The fingerprint (`sha256` of the DDL of both objects) ensures the pair is recreated on any drift, including a change of the MV select in a new service version; reconciliation also restores a missing view.

## 6. Schemas and rules

The wire schema (`deploy/format_schemas/events.proto`) fixes: the messages `PaymentRow`, `LedgerRow`/`LedgerEntryRow`, `PayoutRow`, `ReconAlertRow`, `Money`, `Timestamp`; field numbers correspond to the contracts; enum fields are not decoded into columns (`type`/`severity` omitted in the tables; `direction` - Int32 with a filter). Subqueries in the MVs rename tuples and enum columns: an alias matching a column name shadows it for the remaining SELECT expressions (verified on ClickHouse 24.8). Schema mounting: the service compose uses `deploy/format_schemas`; for an infra-only run of the umbrella stand, copies live in `deploy/clickhouse/schemas` and `deploy/clickhouse/format-config` (sync is checked by `scripts/check-clickhouse.ps1`). Contract changes with new fact fields - additive addition of fields to the schema and tables.

## 7. Edge cases

Redelivery: rows are identical, they collapse with FINAL (11.1); re-running the service does not duplicate tables; changing brokers recreates the Kafka table and MV (fact data is not affected); a broken message - skip with partition continuation; an empty `_headers['event_id']` - the row would be inserted with an empty key (FINAL dedup would not work), therefore the ledger/payment/payout MVs drop rows with empty key fields; events with an empty `transaction_id` (reserve.created/released) create no rows; there are no empty `entries`; overflow of `entry_index` and `decimals` is filtered out; time is converted by seconds plus nanoseconds truncated to milliseconds (DateTime64(3, 'UTC')).

## 8. Non-functional requirements

Pipeline latency - seconds (`kafka_group_name` one per fact, `kafka_num_consumers=1` for dev); reconciliation - seconds at startup; the service does not hold its own Kafka client (less code and dependencies, T-10). Security: the DSN with a password from env is not logged; `clickhouse` in the service compose is a dev stand; the wire schema is a public contract artifact.

## 9. Test plan

| Level | Scenarios |
| :-- | :-- |
| Unit | reconciliation SQL assembly (brokers, schema, comment-fingerprint, filters), env parsing, config validation (including `..` in the schema path and a trailing comma of brokers), DSN error sanitization |
| Integration | testcontainers Kafka+ClickHouse: goose migrations and version; idempotent reconciliation; the payment/ledger/payout/recon event flow (real protobuf from `cbl-protos`): money values, entry_index 0/1, millisecond time precision, `event_type` from the header; event repeat: delivery is confirmed (two rows) and collapses with FINAL; a message with invalid `decimals` is filtered out; a broken message does not stop the partition; shrinking of the broker list (substring) is detected; a deleted MV is restored, fact data is preserved, the flow continues |
| E2E local | compose: up kafka+clickhouse+reporting, topic creation, event producer (Go, `test/e2e/produce`), rows in `fact_*`, values, event repeat (delivery and collapsing), broken message, readyz; down -v |
| E2E global | GE-7 after 11.3/11.4 (end-to-end pipeline), the service stand is checked here |
| Load | not applicable (no HTTP load of the service); 14.2 will cover the pipeline |

Target unit+integration coverage: at least 80%.

## 10. Review (CBL-001, step 6)

Four independent passes (15.10.2026) and closure verification. BLOCKER closed: the reachable vulnerability ch-go v0.63.1 (GO-2025-3603) - upgrade to clickhouse-go v2.34.0/ch-go v0.65.1, govulncheck clean; mojibake of Russian comments in main.go and .env.example and a stray file in the root were removed. MAJOR closed: reconciliation compares the expected DDL by a fingerprint in `COMMENT` (catches shrinking of the broker list, column and select changes) and restores a missing MV; Timestamp nanos are taken into account (millisecond precision confirmed by a test); invalid `decimals` is filtered; runbook/spec are synchronized on the relative `SCHEMA_PATH`; the umbrella stand does not require cloning the services (copies of the schema and config in `deploy/clickhouse`, sync is checked by `check-clickhouse.ps1`); `severity` outside the frozen values does not reach the facts (the `severity` alias shadowed the column - the alerts MV is wrapped in a subquery). MINOR/NIT closed: sanitized DSN error (without a password), 127.0.0.1 in the Kafka test binding, a unique test network name, explicit waiting for duplicate delivery in tests and e2e, `KAFKA_BROKERS` in `.env.example` with the internal address, `recon.alert` in the test producer, doc.go and package comments, `Ready` (readyz and gRPC health reflect the pipeline).

| Role | Key findings | Closure |
| :-- | :-- | :-- |
| No context | MAJOR: substring detection of brokers, the MV is not compared/healed, the decimals filter is declared but missing, spec vs code on SCHEMA_PATH; MINOR: nanos are not taken into account, no behavioral edge tests, weak dedup checks, mojibake, doc.go | fingerprint-COMMENT and heal; filter `<= 18`; spec/schema synchronized; nanos+test; delivery-and-collapsing, decimals case, heal and broker shrinking in integration; encodings; doc.go |
| Architecture | BLOCKER: eternal downtime in a partial state; MAJOR: drift other than brokers, nanos, concurrent replicas; MINOR: severity=0, SCHEMA_PATH, doc.go, mojibake, `.env` brokers, depends_on clickhouse in the umbrella compose, healthcheck, schema parity, recon.alert | heal + fingerprint; single-writer fixed (FR-3, 14.7); nanos; severity filter; doc/`.env`/dependency fixed; schema parity noted (KI-06 copy check); producer fixed |
| With context | BLOCKER: encoding of main.go/.env, junk file; MAJOR: heal/completeness of the comparison, contradiction of the schema format, infra-only stand requires cloning the services; MINOR: depends_on, `.env`, TOCTOU port, network name, DSN password, doc.go, statuses, dim_accounts reference 11.1 | files rewritten; fingerprint/heal; relative path; copies in `deploy/clickhouse` + sync check; depends_on/`.env`/127.0.0.1/unique network; sanitized error; doc.go; 11.1 fixed |
| Security | BLOCKER: GO-2025-3603 (ch-go) reachable - upgrade; MAJOR: DSN password in the error log; MINOR: decimals truncation, substring comparison, 0.0.0.0 binding of the test, junk, mojibake, SCHEMA_PATH, skip policy | upgrade+vulncheck; sanitized error; decimals filter; fingerprint; 127.0.0.1; junk removed; encodings; doc fixed; skip is deliberate (T-RPT-04, KI-reporting-03/04) |

Accepted deliberately: runtime DDL of the pipeline (T-RPT-01, single-writer), delivery loss in the broker change window (fact data is intact), skip_broken without DLQ until 11.3, a dev stand with root+DO_NOT_CHOWN (KI-reporting-02), images without a digest (0.2), retention/PII - 14.7.

## 11. Work plan and commit

Tests first -> migrations and wire schema -> storage (ClickHouse, reconciliation) -> main -> integration/e2e -> runs -> 4 reviews -> AGENTS/README -> commit `CBL-11.2: ClickHouse pipeline: Kafka engine, materialized view and service storage`.

## 12. Open questions and decisions

- Decoding format: wire-compatible flattening instead of mounting `cbl-protos` (ClickHouse does not resolve imports between schema files; verified empirically on 24.8.14).
- `format_schema` - a path relative to `format_schema_path` (the directory is set by the service config; an absolute path is forbidden by the Kafka engine - verified).
- Kafka tables and MVs - runtime reconciliation with a fingerprint in `COMMENT` (T-RPT-01): values are compared, not substrings; healing of a partial state; a single writer.
- Infrastructure stand without cloning the services: copies of the wire schema and config in `deploy/clickhouse` of the umbrella repository, sync - `check-clickhouse.ps1`.
- The `dim_accounts` dictionary and reconciliation - 11.3; `Query`/proxy - 11.4; prod images, CH rights and load - 0.6/14.2/14.7.
