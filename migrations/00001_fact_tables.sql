-- +goose Up
-- ClickHouse reporting schemas (owned by cbl-reporting since CBL-11.2;
-- the cbl-system/deploy/clickhouse init remains the dev bootstrap of the
-- shared stand).
-- The database is created by the environment (CLICKHOUSE_DB in compose) or
-- ops; the CREATE DATABASE below is an idempotent guard for manual
-- application to an existing server.
-- At-least-once deduplication: event_id is part of the ReplacingMergeTree
-- sort key (for alerts alert_id = envelope event_id); identifiers are stored
-- as strings (MV pipeline resilience); readers use FINAL.
CREATE DATABASE IF NOT EXISTS reporting;

CREATE TABLE IF NOT EXISTS reporting.fact_payment_events
(
    event_id        String,
    payment_id      String,
    merchant_id     String,
    event_type      LowCardinality(String),
    expected_amount Int64,
    received_amount Int64,
    fee_amount      Int64,
    asset           LowCardinality(String),
    decimals        UInt8,
    quote_id        String,
    rate            String,
    txid            String,
    state_version   UInt64,
    occurred_at     DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(state_version)
PARTITION BY toYYYYMM(occurred_at)
ORDER BY (merchant_id, payment_id, occurred_at, event_id);

CREATE TABLE IF NOT EXISTS reporting.fact_ledger_entries
(
    event_id       String,
    transaction_id String,
    entry_index    UInt8,
    account_id     String,
    account_type   LowCardinality(String),
    merchant_id    String,
    asset          LowCardinality(String),
    decimals       UInt8,
    direction      Enum8('debit' = 1, 'credit' = 2),
    amount         Int64,
    ref_type       LowCardinality(String),
    ref_id         String,
    event_type     LowCardinality(String),
    occurred_at    DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMM(occurred_at)
ORDER BY (transaction_id, entry_index, event_id);

CREATE TABLE IF NOT EXISTS reporting.fact_payouts
(
    event_id          String,
    payout_id         String,
    merchant_id       String,
    event_type        LowCardinality(String),
    amount            Int64,
    fee_amount        Int64,
    asset             LowCardinality(String),
    decimals          UInt8,
    txid              String,
    provider          LowCardinality(String),
    failure_reason    String,
    refund_invoice_id String,
    state_version     UInt64,
    occurred_at       DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(state_version)
PARTITION BY toYYYYMM(occurred_at)
ORDER BY (merchant_id, payout_id, occurred_at, event_id);

CREATE TABLE IF NOT EXISTS reporting.fact_recon_alerts
(
    alert_id    String,
    alert_type  LowCardinality(String),
    ref_id      String,
    severity    LowCardinality(String),
    expected    String,
    actual      String,
    merchant_id String,
    created_at  DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMM(created_at)
ORDER BY (alert_id);
