package storage

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"
)

// ReportFilter описывает параметры выборки отчета (пустые поля не ограничивают;
// время передается в UTC и биндится драйвером).
type ReportFilter struct {
	// MerchantID задает фильтр мерчанта (пусто - все мерчанты).
	MerchantID string
	// From задает нижнюю границу периода включительно.
	From *time.Time
	// To задает верхнюю границу периода включительно.
	To *time.Time
	// Cursor задает ключи сортировки последней выданной строки.
	Cursor []string
	// Limit задает размер страницы (сервис запрашивает limit+1).
	Limit int
	// GroupBy задает измерение группировки отчета turnover.
	GroupBy string
}

// PaymentFactRow описывает строку отчета payments.
type PaymentFactRow struct {
	PaymentID      string
	MerchantID     string
	EventType      string
	ExpectedAmount int64
	ReceivedAmount int64
	FeeAmount      int64
	Asset          string
	Decimals       uint8
	QuoteID        string
	Rate           string
	TxID           string
	StateVersion   uint64
	OccurredAt     time.Time
	EventID        string
}

// PayoutFactRow описывает строку отчета payouts.
type PayoutFactRow struct {
	PayoutID        string
	MerchantID      string
	EventType       string
	Amount          int64
	FeeAmount       int64
	Asset           string
	Decimals        uint8
	TxID            string
	Provider        string
	FailureReason   string
	RefundInvoiceID string
	StateVersion    uint64
	OccurredAt      time.Time
	EventID         string
}

// LedgerFactRow описывает строку отчета ledger_entries.
type LedgerFactRow struct {
	TransactionID string
	EntryIndex    uint8
	AccountID     string
	AccountType   string
	MerchantID    string
	Asset         string
	Decimals      uint8
	Direction     string
	Amount        int64
	RefType       string
	RefID         string
	EventType     string
	OccurredAt    time.Time
	EventID       string
}

// TurnoverRow описывает строку агрегата отчета turnover.
type TurnoverRow struct {
	Group        string
	Asset        string
	Decimals     uint8
	DebitTotal   int64
	CreditTotal  int64
	EntriesCount int64
}

// reportQuery накапливает фрагменты WHERE и аргументы выборки отчета
// (параметры биндятся драйвером; пользовательский ввод не попадает в SQL).
type reportQuery struct {
	fragments []string
	args      []any
}

// bind добавляет фрагмент условия с аргументами.
func (q *reportQuery) bind(fragment string, args ...any) {
	q.fragments = append(q.fragments, fragment)
	q.args = append(q.args, args...)
}

// merchant добавляет фильтр мерчанта при заданном значении.
func (q *reportQuery) merchant(merchantID string) {
	if merchantID != "" {
		q.bind("merchant_id = ?", merchantID)
	}
}

// period добавляет границы периода включительно (в UTC, точность мс).
func (q *reportQuery) period(from, to *time.Time) {
	if from != nil {
		q.bind("occurred_at >= toDateTime64(?, 3, 'UTC')", from.UTC().Format(timeLayout))
	}
	if to != nil {
		q.bind("occurred_at <= toDateTime64(?, 3, 'UTC')", to.UTC().Format(timeLayout))
	}
}

// where собирает секцию WHERE.
func (q *reportQuery) where() string {
	if len(q.fragments) == 0 {
		return ""
	}
	return " WHERE " + strings.Join(q.fragments, " AND ")
}

// buildReportSQL собирает текст запроса из литералов и плейсхолдеров
// (пользовательский ввод участвует только через bound-параметры).
func buildReportSQL(parts ...string) string {
	return strings.Join(parts, "")
}

// QueryPayments возвращает страницу событий платежей (FINAL).
func QueryPayments(ctx context.Context, db *sql.DB, filter ReportFilter) ([]PaymentFactRow, error) {
	query := &reportQuery{}
	query.merchant(filter.MerchantID)
	query.period(filter.From, filter.To)
	if len(filter.Cursor) == 3 {
		query.bind("(occurred_at, payment_id, event_id) > (toDateTime64(?, 3, 'UTC'), ?, ?)",
			filter.Cursor[0], filter.Cursor[1], filter.Cursor[2])
	}
	query.args = append(query.args, filter.Limit)
	sql := buildReportSQL(`SELECT payment_id, merchant_id, event_type, expected_amount, received_amount, fee_amount,
       asset, decimals, quote_id, rate, txid, state_version, occurred_at, event_id
FROM reporting.fact_payment_events FINAL`, query.where(), `
ORDER BY occurred_at, payment_id, event_id
LIMIT ?`)
	rows, err := db.QueryContext(ctx, sql, query.args...)
	if err != nil {
		return nil, fmt.Errorf("query payments: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var result []PaymentFactRow
	for rows.Next() {
		var row PaymentFactRow
		if err := rows.Scan(&row.PaymentID, &row.MerchantID, &row.EventType, &row.ExpectedAmount,
			&row.ReceivedAmount, &row.FeeAmount, &row.Asset, &row.Decimals, &row.QuoteID, &row.Rate,
			&row.TxID, &row.StateVersion, &row.OccurredAt, &row.EventID); err != nil {
			return nil, fmt.Errorf("scan payment row: %w", err)
		}
		result = append(result, row)
	}
	return result, rows.Err()
}

// QueryPayouts возвращает страницу событий выплат (FINAL).
func QueryPayouts(ctx context.Context, db *sql.DB, filter ReportFilter) ([]PayoutFactRow, error) {
	query := &reportQuery{}
	query.merchant(filter.MerchantID)
	query.period(filter.From, filter.To)
	if len(filter.Cursor) == 3 {
		query.bind("(occurred_at, payout_id, event_id) > (toDateTime64(?, 3, 'UTC'), ?, ?)",
			filter.Cursor[0], filter.Cursor[1], filter.Cursor[2])
	}
	query.args = append(query.args, filter.Limit)
	sql := buildReportSQL(`SELECT payout_id, merchant_id, event_type, amount, fee_amount, asset, decimals,
       txid, provider, failure_reason, refund_invoice_id, state_version, occurred_at, event_id
FROM reporting.fact_payouts FINAL`, query.where(), `
ORDER BY occurred_at, payout_id, event_id
LIMIT ?`)
	rows, err := db.QueryContext(ctx, sql, query.args...)
	if err != nil {
		return nil, fmt.Errorf("query payouts: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var result []PayoutFactRow
	for rows.Next() {
		var row PayoutFactRow
		if err := rows.Scan(&row.PayoutID, &row.MerchantID, &row.EventType, &row.Amount,
			&row.FeeAmount, &row.Asset, &row.Decimals, &row.TxID, &row.Provider, &row.FailureReason,
			&row.RefundInvoiceID, &row.StateVersion, &row.OccurredAt, &row.EventID); err != nil {
			return nil, fmt.Errorf("scan payout row: %w", err)
		}
		result = append(result, row)
	}
	return result, rows.Err()
}

// QueryLedgerEntries возвращает страницу проводок леджера (FINAL, обогащенных
// словарем dim_accounts).
func QueryLedgerEntries(ctx context.Context, db *sql.DB, filter ReportFilter) ([]LedgerFactRow, error) {
	query := &reportQuery{}
	query.merchant(filter.MerchantID)
	query.period(filter.From, filter.To)
	if len(filter.Cursor) == 4 {
		query.bind("(occurred_at, transaction_id, entry_index, event_id) > (toDateTime64(?, 3, 'UTC'), ?, ?, ?)",
			filter.Cursor[0], filter.Cursor[1], filter.Cursor[2], filter.Cursor[3])
	}
	query.args = append(query.args, filter.Limit)
	sql := buildReportSQL(`SELECT transaction_id, entry_index, account_id, account_type, merchant_id, asset, decimals,
       direction, amount, ref_type, ref_id, event_type, occurred_at, event_id
FROM reporting.fact_ledger_entries FINAL`, query.where(), `
ORDER BY occurred_at, transaction_id, entry_index, event_id
LIMIT ?`)
	rows, err := db.QueryContext(ctx, sql, query.args...)
	if err != nil {
		return nil, fmt.Errorf("query ledger entries: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var result []LedgerFactRow
	for rows.Next() {
		var row LedgerFactRow
		if err := rows.Scan(&row.TransactionID, &row.EntryIndex, &row.AccountID, &row.AccountType,
			&row.MerchantID, &row.Asset, &row.Decimals, &row.Direction, &row.Amount, &row.RefType,
			&row.RefID, &row.EventType, &row.OccurredAt, &row.EventID); err != nil {
			return nil, fmt.Errorf("scan ledger row: %w", err)
		}
		result = append(result, row)
	}
	return result, rows.Err()
}

// QueryTurnover возвращает агрегат оборота мерчанта: по активу (group_by
// asset) либо по дню и активу (group_by day_asset); суммы - в минимальных
// единицах одного актива, float не возникает.
func QueryTurnover(ctx context.Context, db *sql.DB, filter ReportFilter) ([]TurnoverRow, error) {
	query := &reportQuery{}
	query.bind("merchant_id = ?", filter.MerchantID)
	query.period(filter.From, filter.To)
	var sql string
	switch filter.GroupBy {
	case "day_asset":
		if len(filter.Cursor) == 2 {
			query.bind("(toString(toDate(occurred_at, 'UTC')), asset) > (?, ?)", filter.Cursor[0], filter.Cursor[1])
		}
		sql = buildReportSQL(`SELECT toString(toDate(occurred_at, 'UTC')) AS grp, asset, any(decimals) AS decimals,
       sumIf(amount, direction = 'debit') AS debit_total,
       sumIf(amount, direction = 'credit') AS credit_total,
       count() AS entries_count
FROM reporting.fact_ledger_entries FINAL`, query.where(), `
GROUP BY grp, asset ORDER BY grp, asset LIMIT ?`)
	default:
		if len(filter.Cursor) == 1 {
			query.bind("asset > ?", filter.Cursor[0])
		}
		sql = buildReportSQL(`SELECT asset AS grp, asset AS asset, any(decimals) AS decimals,
       sumIf(amount, direction = 'debit') AS debit_total,
       sumIf(amount, direction = 'credit') AS credit_total,
       count() AS entries_count
FROM reporting.fact_ledger_entries FINAL`, query.where(), `
GROUP BY grp ORDER BY grp LIMIT ?`)
	}
	query.args = append(query.args, filter.Limit)
	rows, err := db.QueryContext(ctx, sql, query.args...)
	if err != nil {
		return nil, fmt.Errorf("query turnover: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var result []TurnoverRow
	for rows.Next() {
		var row TurnoverRow
		if err := rows.Scan(&row.Group, &row.Asset, &row.Decimals, &row.DebitTotal, &row.CreditTotal, &row.EntriesCount); err != nil {
			return nil, fmt.Errorf("scan turnover row: %w", err)
		}
		result = append(result, row)
	}
	return result, rows.Err()
}
