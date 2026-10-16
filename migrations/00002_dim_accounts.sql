-- +goose Up
-- Account dictionary reporting.dim_accounts (CBL-11.3): merchant accounts are
-- populated from LedgerService.ListMerchantAccounts, platform accounts - by
-- derivation from the deterministic account_id of the chart of accounts
-- (CBL-003, section 9); backfill enriches historical fact_ledger_entries
-- rows, the MV enriches new ones.
-- ReplacingMergeTree(updated_at): dictionary recomputation is idempotent,
-- readers use FINAL (a duplicate before the merge must not multiply the JOIN).
CREATE TABLE IF NOT EXISTS reporting.dim_accounts
(
    account_id   String,
    account_type LowCardinality(String),
    merchant_id  String,
    asset        LowCardinality(String),
    decimals     UInt8,
    updated_at   DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY account_id;
