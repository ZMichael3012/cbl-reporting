package storage

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/cbl/cbl-reporting/internal/domain"
)

// timeLayout задает формат литерала времени ClickHouse для сверки.
const timeLayout = "2006-01-02 15:04:05.000"

// DimAccount описывает строку словаря reporting.dim_accounts.
type DimAccount struct {
	// AccountID задает идентификатор счета.
	AccountID string
	// AccountType задает тип счета плана счетов.
	AccountType string
	// MerchantID задает бизнес-ссылку мерчанта (пусто у системных счетов).
	MerchantID string
	// Asset задает код актива счета.
	Asset string
	// Decimals задает шкалу актива счета.
	Decimals uint8
}

// AssetPair описывает пару (актив, шкала) из фактов леджера.
type AssetPair struct {
	// Asset задает код актива.
	Asset string
	// Decimals задает шкалу актива.
	Decimals uint8
}

// MerchantPair описывает требуемое покрытие словаря из фактов: платежи
// требуют счет `merchant`, выплаты - `payout` по паре (актив, шкала).
type MerchantPair struct {
	// MerchantID задает бизнес-ссылку мерчанта.
	MerchantID string
	// Asset задает код актива.
	Asset string
	// Decimals задает шкалу актива.
	Decimals uint8
	// AccountType задает тип требуемой словарной строки.
	AccountType string
}

// TxAggregate описывает агрегат проводок по транзакции (одна сторона сверки).
type TxAggregate struct {
	// Count задает число проводок.
	Count int64
	// Debit задает сумму проводок дебета в минимальных единицах.
	Debit int64
	// Credit задает сумму проводок кредита в минимальных единицах.
	Credit int64
}

// FactMerchantPairs возвращает требуемое покрытие словаря по мерчантам из
// фактов (FINAL): платежи - счет merchant, выплаты - счет payout. Пара без
// строки словаря означает, что мерчанта нужно (пере)синхронизировать с Ledger
// (ловит и новые активы известных мерчантов).
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

// DimPairCoverage возвращает покрытие словаря по парам (мерчант, актив, шкала,
// тип) для сверки с требуемым покрытием фактов.
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

// FactAssetPairs возвращает различные пары (asset, decimals) из фактов леджера
// (FINAL) - кандидаты деривации системных счетов.
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

// DimExistingIDs возвращает подмножество идентификаторов, присутствующих в
// словаре (FINAL).
func DimExistingIDs(ctx context.Context, db *sql.DB, accountIDs []string) (map[string]struct{}, error) {
	existing := make(map[string]struct{})
	if len(accountIDs) == 0 {
		return existing, nil
	}
	args := make([]any, 0, len(accountIDs))
	for _, id := range accountIDs {
		args = append(args, id)
	}
	//nolint:gosec // в строку подставляются только плейсхолдеры, значения в аргументах
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

// DimAccountsByIDs возвращает строки словаря по идентификаторам (FINAL).
func DimAccountsByIDs(ctx context.Context, db *sql.DB, accountIDs []string) (map[string]DimAccount, error) {
	accounts := make(map[string]DimAccount)
	if len(accountIDs) == 0 {
		return accounts, nil
	}
	args := make([]any, 0, len(accountIDs))
	for _, id := range accountIDs {
		args = append(args, id)
	}
	//nolint:gosec // в строку подставляются только плейсхолдеры, значения в аргументах
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

// DimAccountPage возвращает страницу идентификаторов словаря после курсора
// (round-robin обхода сверки).
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

// InsertDimAccounts вставляет строки словаря батчами (значения валидированы
// вызывающим: тип счета из плана, ссылка мерчанта и актив по правилам; биндинг
// параметров - штатный механизм драйвера; батч ограничивает число партов).
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
		//nolint:gosec // в строку подставляются только плейсхолдеры, значения в аргументах
		query := `INSERT INTO reporting.dim_accounts
(account_id, account_type, merchant_id, asset, decimals, updated_at) VALUES ` + strings.Join(placeholders, ",")
		if _, err := db.ExecContext(ctx, query, args...); err != nil {
			return fmt.Errorf("insert dim accounts: %w", err)
		}
	}
	return nil
}

// FactUnenrichedAccounts возвращает счета словаря с пустым account_type в
// фактах леджера (FINAL, детерминированный порядок, лимит на цикл): счета вне
// словаря не выбираются, поэтому бюджет цикла не голодает на legacy-строках.
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

// BackfillAccount обогащает исторические строки фактов леджера значениями
// словаря (мутация по узкому предикату, идемпотентна и сходится; параметры
// биндятся драйвером).
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
	// Значение account_type = '' в предикате гарантирует сходимость: уже
	// обогащенные строки повторно не переписываются.
	_, err := db.ExecContext(ctx, `ALTER TABLE reporting.fact_ledger_entries
UPDATE account_type = ?, merchant_id = ?
WHERE account_id = ? AND account_type = ''`,
		account.AccountType, account.MerchantID, account.AccountID)
	if err != nil {
		return fmt.Errorf("backfill account %s: %w", account.AccountID, err)
	}
	return nil
}

// FactsAggregate агрегирует проводки фактов по транзакциям за окно
// [from, to) для сверки с Ledger.
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
