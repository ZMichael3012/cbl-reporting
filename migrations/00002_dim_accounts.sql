-- +goose Up
-- Словарь счетов reporting.dim_accounts (CBL-11.3): мерчантские счета
-- наполняются из LedgerService.ListMerchantAccounts, системные - деривацией по
-- детерминированным account_id плана счетов (CBL-003, раздел 9); backfill
-- обогащает исторические строки fact_ledger_entries, MV - новые.
-- ReplacingMergeTree(updated_at): пересчет словаря идемпотентен, читатели
-- используют FINAL (дубль до слияния не должен размножать JOIN).
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
