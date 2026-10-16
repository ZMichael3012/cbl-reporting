package storage

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"
)

// ReportFilter describes report query parameters (empty fields do not
// constrain; time is passed in UTC and bound by the driver).
type ReportFilter struct {
	// MerchantID defines the merchant filter (empty - all merchants).
	MerchantID string
	// From defines the inclusive lower period bound.
	From *time.Time
	// To defines the inclusive upper period bound.
	To *time.Time
	// Cursor defines the sort keys of the last returned row.
	Cursor []string
	// Limit defines the page size (the service requests limit+1).
	Limit int
	// GroupBy defines the grouping dimension of the turnover report.
	GroupBy string
}

// PaymentFactRow describes a row of the payments report.
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

// PayoutFactRow describes a row of the payouts report.
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

// LedgerFactRow describes a row of the ledger_entries report.
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

// TurnoverRow describes an aggregate row of the turnover report.
type TurnoverRow struct {
	Group        string
	Asset        string
	Decimals     uint8
	DebitTotal   int64
	CreditTotal  int64
	EntriesCount int64
}

// reportQuery accumulates WHERE fragments and report query arguments
// (parameters are bound by the driver; user input does not reach SQL).
type reportQuery struct {
	fragments []string
	args      []any
}

// bind adds a condition fragment with arguments.
func (q *reportQuery) bind(fragment string, args ...any) {
	q.fragments = append(q.fragments, fragment)
	q.args = append(q.args, args...)
}

// merchant adds the merchant filter when a value is set.
func (q *reportQuery) merchant(merchantID string) {
	if merchantID != "" {
		q.bind("merchant_id = ?", merchantID)
	}
}

// period adds inclusive period bounds (in UTC, millisecond precision).
func (q *reportQuery) period(from, to *time.Time) {
	if from != nil {
		q.bind("occurred_at >= toDateTime64(?, 3, 'UTC')", from.UTC().Format(timeLayout))
	}
	if to != nil {
		q.bind("occurred_at <= toDateTime64(?, 3, 'UTC')", to.UTC().Format(timeLayout))
	}
}

// where builds the WHERE section.
func (q *reportQuery) where() string {
	if len(q.fragments) == 0 {
		return ""
	}
	return " WHERE " + strings.Join(q.fragments, " AND ")
}

// buildReportSQL builds the query text from literals and placeholders
// (user input participates only via bound parameters).
func buildReportSQL(parts ...string) string {
	return strings.Join(parts, "")
}

// QueryPayments returns a page of payment events (FINAL).
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

// QueryPayouts returns a page of payout events (FINAL).
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

// QueryLedgerEntries returns a page of ledger entries (FINAL, enriched by the
// dim_accounts dictionary).
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

// QueryTurnover returns the merchant turnover aggregate: per asset (group_by
// asset) or per day and asset (group_by day_asset); the sums are in minor
// units of one asset, no float occurs.
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
