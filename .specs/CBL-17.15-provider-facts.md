# CBL-17.15 (reporting increment) - Provider, raw status and fee dimensions

Task: CBL-17.15 in `CBL-004` (E17), reporting part of "provider reporting and
administration"; depends on 17.10. Repository: `cbl-reporting`. Status: in progress.

## 1. Problem and scope

`payment.events` carries `provider`, `raw_status` and the fee split (CF-4, fields 12 - 16),
but the ClickHouse payment fact stores only the total fee and no provider or raw status:
provider-level reporting and fee attribution are impossible. In scope: the payment fact
columns, the MR pipeline mapping, the dashboard dimensions, the wire-schema copies and the
integration assertion. Out of scope: new gRPC report types (additive contract task), payout
raw status (the PayoutEvent contract has no such field), retention (14.7) and the
back-office `EnableProvider` route (the connector and back-office increments under the same
task).

## 2. Architecture

- Migration `00003_provider_payment_dimensions.sql`: additive columns on
  `fact_payment_events`: `provider LowCardinality(String)`, `raw_status
  LowCardinality(String)`, `gateway_fee Int64`, `network_fee Int64`, `fx_spread Int64`
  (defaults empty/zero, `AFTER` the fee column).
- Wire schema `deploy/format_schemas/events.proto`: PaymentRow gains fields 12 - 16 with
  the contract numbers/types; the umbrella copy `deploy/clickhouse/schemas/events.proto`
  is updated identically and the SHA check stays green.
- Pipeline spec: the Kafka payment columns gain the typed fields; the MV projects the fee
  split first elements alongside `fee_amount`.
- Dashboard `cbl-payments.json`: a provider/raw-status breakdown panel and a fee-split
  panel; the artifact test enforces SELECT-only, FINAL, time/asset filters and exact money
  scaling (the fee components are summed and scaled like the total fee).

## 3. Test plan

| ID | Case | Expectation |
| :-- | :-- | :-- |
| F1 | Kafka -> fact mapping | a payment event with provider/raw_status/fee split lands in all five columns |
| F2 | migration | the five columns exist with the defaults; a re-run is idempotent |
| F3 | MV DDL | the payment columns and SELECT contain the new fields |
| F4 | dashboards | the two panels pass the artifact rules (time/asset/merchant filters, FINAL, exact scaling) |
| F5 | empty/absent provider in an old event | empty string, zero fees (backward compatible wire decode) |

Integration: the Kafka+ClickHouse pipeline test extends the payment assertion (F1) and the
invalid/duplicate filter rows still pass.
E2E: the reporting dashboards scenario remains green; the pipeline change is built and
tested on testcontainers (no stand-specific change).

## 4. Red evidence

The acceptance assertion queries the five new columns and was written together with the
migration and the pipeline mapping; the integration test was not run in a red state
separately (a ClickHouse column addition is not reverted between runs on the same
stand). The first full integration run after the change is the acceptance evidence; the
unit and artifact tests were green throughout. This deviation is recorded rather than
claimed as a red run.

## 5. Results and review closure (20.10.2026)

- F1 - F5 green: the Kafka+ClickHouse integration test inserts a payment with provider,
  raw_status and the three fee components and reads all five columns; the migration
  applies as version 3; the MV DDL assertions cover the new mapping; the two dashboard
  panels pass the artifact rules (FINAL, time/asset/merchant filters, exact scaling).
- Focused stand run `20261020t082822z`: `/readyz` 200, migration v3, five columns
  present, both panel SQLs execute against the real schema, Grafana serves the updated
  dashboard with both panels; the full data-flow scenario stays covered by the
  integration test and the release run.
- Gates: golangci-lint 0, go vet clean, em dash check green; the umbrella copy of the
  wire schema has the same SHA256 as the service copy.
- Review pass 1 (no-context): the field numbers/types match `payment_events.proto` fields
  12 - 16; defaults keep old rows and events decodable.
- Review pass 2 (architecture): additive migration, MV and wire schema in sync; the facts
  remain the only ClickHouse storage; reports are untouched (no contract change).
- Review pass 3 (with-context, blocking): the pipeline fingerprint changes, so the
  service recreates the MV on startup; the Kafka table is recreated without touching
  facts. Closed: the integration test proves the new mapping after reconciliation; the
  migration is forward-only and idempotent (`IF NOT EXISTS`).
- Review pass 4 (security): no new input paths; the panels are read-only, single
  statement, time/asset/merchant scoped; fee components are Int64 minor units, no
  float (dashboard SQL uses divideDecimal).
