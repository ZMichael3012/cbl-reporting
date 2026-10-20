-- +goose Up
-- Provider attribution and fee split of payment events (CF-4 fields 12 - 16,
-- CBL-17.15). The defaults keep older events decodable and the rows already
-- stored readable; the columns are additive and never rewritten retroactively.
ALTER TABLE reporting.fact_payment_events
    ADD COLUMN IF NOT EXISTS provider    LowCardinality(String) DEFAULT '' AFTER fee_amount,
    ADD COLUMN IF NOT EXISTS raw_status  LowCardinality(String) DEFAULT '' AFTER provider,
    ADD COLUMN IF NOT EXISTS gateway_fee Int64 DEFAULT 0 AFTER raw_status,
    ADD COLUMN IF NOT EXISTS network_fee Int64 DEFAULT 0 AFTER gateway_fee,
    ADD COLUMN IF NOT EXISTS fx_spread   Int64 DEFAULT 0 AFTER network_fee;

-- +goose Down
ALTER TABLE reporting.fact_payment_events
    DROP COLUMN IF EXISTS fx_spread,
    DROP COLUMN IF EXISTS network_fee,
    DROP COLUMN IF EXISTS gateway_fee,
    DROP COLUMN IF EXISTS raw_status,
    DROP COLUMN IF EXISTS provider;
