package storage

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/cbl/cbl-reporting/internal/domain"
)

// timeLayout defines the ClickHouse time literal format for reconciliation.
const timeLayout = "2006-01-02 15:04:05.000"

// DimAccount describes a row of the reporting.dim_accounts dictionary.
type DimAccount struct {
	// AccountID defines the account identifier.
	AccountID string
	// AccountType defines the account type of the chart of accounts.
	AccountType string
	// MerchantID defines the merchant business reference (empty for platform accounts).
	MerchantID string
	// Asset defines the account asset code.
	Asset string
	// Decimals defines the account asset scale.
	Decimals uint8
}

// AssetPair describes an (asset, scale) pair from ledger facts.
type AssetPair struct {
	// Asset defines the asset code.
	Asset string
	// Decimals defines the asset scale.
	Decimals uint8
}

// MerchantPair describes the required dictionary coverage from facts: payments
// require the `merchant` account, payouts - `payout`, per (asset, scale) pair.
type MerchantPair struct {
	// MerchantID defines the merchant business reference.
	MerchantID string
	// Asset defines the asset code.
	Asset string
	// Decimals defines the asset scale.
	Decimals uint8
	// AccountType defines the type of the required dictionary row.
	AccountType string
}

// TxAggregate describes an aggregate of entries per transaction (one side of reconciliation).
type TxAggregate struct {
	// Count defines the number of entries.
	Count int64
	// Debit defines the total debit entries in minor units.
	Debit int64
	// Credit defines the total credit entries in minor units.
	Credit int64
}

// FactMerchantPairs returns the required dictionary coverage by merchants from
// facts (FINAL): payments - the merchant account, payouts - the payout account.
// A pair without a dictionary row means the merchant must be (re)synchronized
// with Ledger (this also catches new assets of known merchants).
func FactMerchantPairs(ctx context.Context, db *sql.DB) ([]MerchantPair, error) {
	rows, err := db.QueryContext(ctx, `SELECT DISTINCT merchant_id, asset, decimals, 'merchant' AS account_type
FROM reporting.fact_payment_events FINAL WHERE merchant_id != '' AND asset != ''
UNION ALL
SELECT DISTINCT merchant_id, asset, decimals, 'payout' AS account_type
FROM reporting.fact_payouts FINAL WHERE merchant_id != '' AND asset != ''`)
	if err != nil {
		return nil, fmt.Errorf("select fact merchant pairs: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var pairs []MerchantPair
	for rows.Next() {
		var pair MerchantPair
		if err := rows.Scan(&pair.MerchantID, &pair.Asset, &pair.Decimals, &pair.AccountType); err != nil {
			return nil, fmt.Errorf("scan merchant pair: %w", err)
		}
		pairs = append(pairs, pair)
	}
	return pairs, rows.Err()
}

// DimPairCoverage returns the dictionary coverage by (merchant, asset, scale,
// type) pairs for comparison with the required coverage from facts.
func DimPairCoverage(ctx context.Context, db *sql.DB) (map[MerchantPair]struct{}, error) {
	rows, err := db.QueryContext(ctx, `SELECT DISTINCT merchant_id, asset, decimals, account_type
FROM reporting.dim_accounts FINAL
WHERE account_type IN ('merchant', 'payout') AND merchant_id != ''`)
	if err != nil {
		return nil, fmt.Errorf("select dim pair coverage: %w", err)
	}
	defer func() { _ = rows.Close() }()
	coverage := make(map[MerchantPair]struct{})
	for rows.Next() {
		var pair MerchantPair
		if err := rows.Scan(&pair.MerchantID, &pair.Asset, &pair.Decimals, &pair.AccountType); err != nil {
			return nil, fmt.Errorf("scan dim pair: %w", err)
		}
		coverage[pair] = struct{}{}
	}
	return coverage, rows.Err()
}

// FactAssetPairs returns the distinct (asset, decimals) pairs from ledger facts
// (FINAL) - candidates for platform account derivation.
func FactAssetPairs(ctx context.Context, db *sql.DB) ([]AssetPair, error) {
	rows, err := db.QueryContext(ctx,
		`SELECT DISTINCT asset, decimals FROM reporting.fact_ledger_entries FINAL WHERE asset != '' ORDER BY asset, decimals`)
	if err != nil {
		return nil, fmt.Errorf("select fact asset pairs: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var pairs []AssetPair
	for rows.Next() {
		var pair AssetPair
		if err := rows.Scan(&pair.Asset, &pair.Decimals); err != nil {
			return nil, fmt.Errorf("scan asset pair: %w", err)
		}
		pairs = append(pairs, pair)
	}
	return pairs, rows.Err()
}

// DimExistingIDs returns the subset of identifiers that are present in the
// dictionary (FINAL).
func DimExistingIDs(ctx context.Context, db *sql.DB, accountIDs []string) (map[string]struct{}, error) {
	existing := make(map[string]struct{})
	if len(accountIDs) == 0 {
		return existing, nil
	}
	args := make([]any, 0, len(accountIDs))
	for _, id := range accountIDs {
		args = append(args, id)
	}
	//nolint:gosec // only placeholders are interpolated into the string, values stay in arguments
	query := `SELECT account_id FROM reporting.dim_accounts FINAL WHERE account_id IN (` +
		strings.TrimSuffix(strings.Repeat("?,", len(args)), ",") + `)`
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("select dim existing ids: %w", err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var accountID string
		if err := rows.Scan(&accountID); err != nil {
			return nil, fmt.Errorf("scan dim id: %w", err)
		}
		existing[accountID] = struct{}{}
	}
	return existing, rows.Err()
}

// DimAccountsByIDs returns dictionary rows by identifiers (FINAL).
func DimAccountsByIDs(ctx context.Context, db *sql.DB, accountIDs []string) (map[string]DimAccount, error) {
	accounts := make(map[string]DimAccount)
	if len(accountIDs) == 0 {
		return accounts, nil
	}
	args := make([]any, 0, len(accountIDs))
	for _, id := range accountIDs {
		args = append(args, id)
	}
	//nolint:gosec // only placeholders are interpolated into the string, values stay in arguments
	query := `SELECT account_id, account_type, merchant_id, asset, decimals
FROM reporting.dim_accounts FINAL WHERE account_id IN (` +
		strings.TrimSuffix(strings.Repeat("?,", len(args)), ",") + `)`
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("select dim accounts: %w", err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var account DimAccount
		if err := rows.Scan(&account.AccountID, &account.AccountType, &account.MerchantID, &account.Asset, &account.Decimals); err != nil {
			return nil, fmt.Errorf("scan dim account: %w", err)
		}
		accounts[account.AccountID] = account
	}
	return accounts, rows.Err()
}

// DimAccountPage returns a page of dictionary identifiers after the cursor
// (round-robin reconciliation walk).
func DimAccountPage(ctx context.Context, db *sql.DB, after string, limit int) ([]string, error) {
	rows, err := db.QueryContext(ctx,
		`SELECT account_id FROM reporting.dim_accounts FINAL
WHERE account_id != '' AND account_id > ? ORDER BY account_id LIMIT ?`, after, limit)
	if err != nil {
		return nil, fmt.Errorf("select dim page: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var ids []string
	for rows.Next() {
		var accountID string
		if err := rows.Scan(&accountID); err != nil {
			return nil, fmt.Errorf("scan dim page id: %w", err)
		}
		ids = append(ids, accountID)
	}
	return ids, rows.Err()
}

// InsertDimAccounts inserts dictionary rows in batches (values are validated
// by the caller: the account type is from the chart of accounts, the merchant
// reference and the asset follow the rules; parameter binding is the driver
// mechanism; the batch limits the number of parts).
func InsertDimAccounts(ctx context.Context, db *sql.DB, accounts []DimAccount) error {
	const batchSize = 200
	for start := 0; start < len(accounts); start += batchSize {
		end := min(start+batchSize, len(accounts))
		chunk := accounts[start:end]
		placeholders := make([]string, 0, len(chunk))
		args := make([]any, 0, len(chunk)*5)
		for _, account := range chunk {
			placeholders = append(placeholders, "(?, ?, ?, ?, ?, now64(3))")
			args = append(args, account.AccountID, account.AccountType, account.MerchantID, account.Asset, account.Decimals)
		}
		//nolint:gosec // only placeholders are interpolated into the string, values stay in arguments
		query := `INSERT INTO reporting.dim_accounts
(account_id, account_type, merchant_id, asset, decimals, updated_at) VALUES ` + strings.Join(placeholders, ",")
		if _, err := db.ExecContext(ctx, query, args...); err != nil {
			return fmt.Errorf("insert dim accounts: %w", err)
		}
	}
	return nil
}

// FactUnenrichedAccounts returns dictionary accounts with an empty
// account_type in the ledger facts (FINAL, deterministic order, per-cycle
// limit): accounts outside the dictionary are not selected, so the cycle
// budget is not starved by legacy rows.
func FactUnenrichedAccounts(ctx context.Context, db *sql.DB, limit int) ([]string, error) {
	rows, err := db.QueryContext(ctx,
		`SELECT DISTINCT account_id FROM reporting.fact_ledger_entries FINAL
WHERE account_type = '' AND account_id IN (SELECT account_id FROM reporting.dim_accounts FINAL)
ORDER BY account_id LIMIT ?`, limit)
	if err != nil {
		return nil, fmt.Errorf("select unenriched accounts: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var ids []string
	for rows.Next() {
		var accountID string
		if err := rows.Scan(&accountID); err != nil {
			return nil, fmt.Errorf("scan unenriched account: %w", err)
		}
		ids = append(ids, accountID)
	}
	return ids, rows.Err()
}

// BackfillAccount enriches historical ledger fact rows with dictionary values
// (a mutation by a narrow predicate, idempotent and convergent; parameters
// are bound by the driver).
func BackfillAccount(ctx context.Context, db *sql.DB, account DimAccount) error {
	if !domain.ValidAccountType(account.AccountType) {
		return fmt.Errorf("unknown account type %q in dim", account.AccountType)
	}
	personal := account.AccountType == "merchant" || account.AccountType == "payout"
	if personal && !domain.ValidMerchantRef(account.MerchantID) {
		return fmt.Errorf("unexpected merchant id %q in dim", account.MerchantID)
	}
	if !personal && account.MerchantID != "" {
		return fmt.Errorf("platform account %s must not carry merchant id", account.AccountType)
	}
	// The account_type = '' value in the predicate guarantees convergence:
	// already enriched rows are not rewritten again.
	_, err := db.ExecContext(ctx, `ALTER TABLE reporting.fact_ledger_entries
UPDATE account_type = ?, merchant_id = ?
WHERE account_id = ? AND account_type = ''`,
		account.AccountType, account.MerchantID, account.AccountID)
	if err != nil {
		return fmt.Errorf("backfill account %s: %w", account.AccountID, err)
	}
	return nil
}

// FactsAggregate aggregates fact entries by transaction over the window
// [from, to) for reconciliation with Ledger.
func FactsAggregate(ctx context.Context, db *sql.DB, accountID string, from, to time.Time) (map[string]TxAggregate, error) {
	rows, err := db.QueryContext(ctx,
		`SELECT transaction_id, count() AS cnt,
       sumIf(amount, direction = 'debit') AS debit,
       sumIf(amount, direction = 'credit') AS credit
FROM reporting.fact_ledger_entries FINAL
WHERE account_id = ? AND occurred_at >= toDateTime64(?, 3, 'UTC') AND occurred_at < toDateTime64(?, 3, 'UTC')
GROUP BY transaction_id`, accountID, from.UTC().Format(timeLayout), to.UTC().Format(timeLayout))
	if err != nil {
		return nil, fmt.Errorf("select facts aggregate: %w", err)
	}
	defer func() { _ = rows.Close() }()
	aggregates := make(map[string]TxAggregate)
	for rows.Next() {
		var transactionID string
		var aggregate TxAggregate
		if err := rows.Scan(&transactionID, &aggregate.Count, &aggregate.Debit, &aggregate.Credit); err != nil {
			return nil, fmt.Errorf("scan facts aggregate: %w", err)
		}
		aggregates[transactionID] = aggregate
	}
	return aggregates, rows.Err()
}
