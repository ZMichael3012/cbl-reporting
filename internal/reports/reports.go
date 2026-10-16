// Package reports implements the ReportingService report catalog: validation,
// keyset pagination, ClickHouse queries and value formatting (CBL-11.4).
package reports

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/cbl/cbl-reporting/internal/storage"
)

// Domain errors of the service (the transport maps them to gRPC codes).
var (
	// ErrUnknownReport means a report type outside the closed catalog.
	ErrUnknownReport = errors.New("unknown report type")
	// ErrInvalidArgument means invalid request parameters.
	ErrInvalidArgument = errors.New("invalid argument")
)

// Contract (billing/reporting/v1) and transport limits.
const (
	defaultLimit    = 20
	maxLimit        = 100
	maxCursorLength = 512
	cursorVersion   = 1
)

// cursorTimeLayout defines the time format in cursor keys (precision of the
// DateTime64(3) columns).
const cursorTimeLayout = "2006-01-02 15:04:05.000"

// cursorDayLayout defines the day format in cursor keys of the day_asset grouping.
const cursorDayLayout = "2006-01-02"

// cursorKeyKind defines the validated form of a cursor key.
type cursorKeyKind int

const (
	// keyTime defines time in the fact cursor format.
	keyTime cursorKeyKind = iota
	// keyDay defines a day in the 2006-01-02 format.
	keyDay
	// keyIndex defines the entry index (UInt8).
	keyIndex
	// keyText defines a string key (identifier, asset).
	keyText
)

// reportDef describes a catalog entry: the grouping, the merchant requirement
// and cursor key formats per group_by variant ("" - a type without grouping).
type reportDef struct {
	groupBy          []string
	defaultGroupBy   string
	requiresMerchant bool
	cursorKeys       map[string][]cursorKeyKind
}

// catalog defines the closed report catalog of 11.4.
var catalog = map[string]reportDef{
	"payments":       {cursorKeys: map[string][]cursorKeyKind{"": {keyTime, keyText, keyText}}},
	"payouts":        {cursorKeys: map[string][]cursorKeyKind{"": {keyTime, keyText, keyText}}},
	"ledger_entries": {cursorKeys: map[string][]cursorKeyKind{"": {keyTime, keyText, keyIndex, keyText}}},
	"turnover": {
		groupBy:          []string{"asset", "day_asset"},
		defaultGroupBy:   "asset",
		requiresMerchant: true,
		cursorKeys: map[string][]cursorKeyKind{
			"asset":     {keyText},
			"day_asset": {keyDay, keyText},
		},
	},
}

// Reports returns the catalog types in alphabetical order (documentation and diagnostics).
func Reports() []string {
	names := make([]string, 0, len(catalog))
	for name := range catalog {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// QueryRequest describes report parameters; the period is aligned to
// milliseconds (DateTime64(3) precision).
type QueryRequest struct {
	// ReportType defines the report type from the catalog.
	ReportType string
	// MerchantID defines the merchant (empty - all merchants for list reports).
	MerchantID string
	// PeriodFrom defines the inclusive lower period bound.
	PeriodFrom *time.Time
	// PeriodTo defines the inclusive upper period bound.
	PeriodTo *time.Time
	// GroupBy defines the grouping dimension (turnover).
	GroupBy string
	// Limit defines the page size (0 - default 20, maximum 100).
	Limit uint32
	// Cursor defines the opaque continuation cursor.
	Cursor string
}

// Page describes a report page.
type Page struct {
	// Rows defines the page rows (values are strings).
	Rows []map[string]string
	// NextCursor defines the next page cursor (empty - the end).
	NextCursor string
}

// Service executes report queries over the catalog.
type Service struct {
	db *sql.DB
}

// New assembles the reports service.
func New(db *sql.DB) (*Service, error) {
	if db == nil {
		return nil, errors.New("clickhouse db is required")
	}
	return &Service{db: db}, nil
}

// Query validates parameters, executes the query and returns a report page.
func (s *Service) Query(ctx context.Context, request QueryRequest) (Page, error) {
	def, ok := catalog[request.ReportType]
	if !ok {
		return Page{}, fmt.Errorf("%w: %q", ErrUnknownReport, request.ReportType)
	}
	if err := validate(request, def); err != nil {
		return Page{}, err
	}
	groupBy, err := resolveGroupBy(request.GroupBy, def)
	if err != nil {
		return Page{}, err
	}
	limit := int(request.Limit)
	if limit == 0 {
		limit = defaultLimit
	}
	filter := storage.ReportFilter{
		MerchantID: request.MerchantID,
		From:       truncateMilli(request.PeriodFrom),
		To:         truncateMilli(request.PeriodTo),
		Limit:      limit + 1,
		GroupBy:    groupBy,
	}
	if request.Cursor != "" {
		keys, err := decodeCursor(request.Cursor, request.ReportType, def.cursorKeys[groupBy])
		if err != nil {
			return Page{}, err
		}
		filter.Cursor = keys
	}
	switch request.ReportType {
	case "payments":
		return s.queryPayments(ctx, filter, limit)
	case "payouts":
		return s.queryPayouts(ctx, filter, limit)
	case "ledger_entries":
		return s.queryLedgerEntries(ctx, filter, limit)
	default:
		return s.queryTurnover(ctx, filter, limit, groupBy)
	}
}

// validate checks request parameters against a catalog entry.
func validate(request QueryRequest, def reportDef) error {
	if request.Limit > maxLimit {
		return fmt.Errorf("%w: limit %d exceeds %d", ErrInvalidArgument, request.Limit, maxLimit)
	}
	if len(request.Cursor) > maxCursorLength {
		return fmt.Errorf("%w: cursor exceeds %d characters", ErrInvalidArgument, maxCursorLength)
	}
	if request.PeriodFrom != nil && request.PeriodTo != nil && request.PeriodTo.Before(*request.PeriodFrom) {
		return fmt.Errorf("%w: period_to is before period_from", ErrInvalidArgument)
	}
	if def.requiresMerchant && request.MerchantID == "" {
		return fmt.Errorf("%w: report %q requires merchant_id", ErrInvalidArgument, request.ReportType)
	}
	return nil
}

// resolveGroupBy validates the grouping via the catalog whitelist.
func resolveGroupBy(groupBy string, def reportDef) (string, error) {
	if def.groupBy == nil {
		if groupBy != "" {
			return "", fmt.Errorf("%w: group_by is not supported for this report", ErrInvalidArgument)
		}
		return "", nil
	}
	if groupBy == "" {
		return def.defaultGroupBy, nil
	}
	for _, allowed := range def.groupBy {
		if groupBy == allowed {
			return groupBy, nil
		}
	}
	return "", fmt.Errorf("%w: group_by %q is not supported", ErrInvalidArgument, groupBy)
}

// truncateMilli aligns a period bound to the DateTime64(3) column precision.
func truncateMilli(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	truncated := value.UTC().Truncate(time.Millisecond)
	return &truncated
}

// cursorPayload describes the opaque cursor (keyset over sort keys).
type cursorPayload struct {
	Version int      `json:"v"`
	Report  string   `json:"report"`
	Keys    []string `json:"keys"`
}

// encodeCursor builds a cursor from the sort keys of the last returned row.
func encodeCursor(report string, keys []string) (string, error) {
	payload, err := json.Marshal(cursorPayload{Version: cursorVersion, Report: report, Keys: keys})
	if err != nil {
		return "", fmt.Errorf("encode cursor: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(payload), nil
}

// decodeCursor parses and validates a cursor: the structure (version, report,
// number of keys) and the format of each key (time, day, index, string ID).
func decodeCursor(raw, report string, kinds []cursorKeyKind) ([]string, error) {
	decoded, err := base64.RawURLEncoding.DecodeString(raw)
	if err != nil {
		return nil, fmt.Errorf("%w: cursor is not decodable", ErrInvalidArgument)
	}
	var payload cursorPayload
	if err := json.Unmarshal(decoded, &payload); err != nil {
		return nil, fmt.Errorf("%w: cursor is malformed", ErrInvalidArgument)
	}
	if payload.Version != cursorVersion || payload.Report != report || len(payload.Keys) != len(kinds) {
		return nil, fmt.Errorf("%w: cursor does not belong to this report", ErrInvalidArgument)
	}
	for index, kind := range kinds {
		if err := validateCursorKey(kind, payload.Keys[index]); err != nil {
			return nil, err
		}
	}
	return payload.Keys, nil
}

// validateCursorKey checks the format of a single cursor key: a structurally
// valid but corrupted cursor must not reach ClickHouse.
func validateCursorKey(kind cursorKeyKind, value string) error {
	switch kind {
	case keyTime:
		if _, err := time.ParseInLocation(cursorTimeLayout, value, time.UTC); err != nil {
			return fmt.Errorf("%w: cursor timestamp is malformed", ErrInvalidArgument)
		}
	case keyDay:
		if _, err := time.ParseInLocation(cursorDayLayout, value, time.UTC); err != nil {
			return fmt.Errorf("%w: cursor day is malformed", ErrInvalidArgument)
		}
	case keyIndex:
		if _, err := strconv.ParseUint(value, 10, 8); err != nil {
			return fmt.Errorf("%w: cursor index is malformed", ErrInvalidArgument)
		}
	case keyText:
		if value == "" || len(value) > 128 {
			return fmt.Errorf("%w: cursor key is malformed", ErrInvalidArgument)
		}
	}
	return nil
}

// FormatMinor converts an amount in minor units into the canonical decimal
// string ^-?(0|[1-9][0-9]*)(\.[0-9]+)?$ (float is forbidden).
func FormatMinor(minor int64, decimals uint8) string {
	digits := strconv.FormatInt(minor, 10)
	negative := strings.HasPrefix(digits, "-")
	if negative {
		digits = digits[1:]
	}
	dec := int(decimals)
	if dec > 0 {
		if len(digits) <= dec {
			digits = strings.Repeat("0", dec-len(digits)+1) + digits
		}
		point := len(digits) - dec
		digits = digits[:point] + "." + digits[point:]
	}
	if negative {
		return "-" + digits
	}
	return digits
}

// formatTime converts time to RFC 3339 UTC with milliseconds.
func formatTime(value time.Time) string {
	return value.UTC().Format("2006-01-02T15:04:05.000Z07:00")
}

// queryPayments returns a page of the payments report.
func (s *Service) queryPayments(ctx context.Context, filter storage.ReportFilter, limit int) (Page, error) {
	rows, err := storage.QueryPayments(ctx, s.db, filter)
	if err != nil {
		return Page{}, err
	}
	page := Page{Rows: make([]map[string]string, 0, min(len(rows), limit))}
	for index, row := range rows {
		if index == limit {
			break
		}
		page.Rows = append(page.Rows, map[string]string{
			"payment_id":      row.PaymentID,
			"merchant_id":     row.MerchantID,
			"event_type":      row.EventType,
			"expected_amount": FormatMinor(row.ExpectedAmount, row.Decimals),
			"received_amount": FormatMinor(row.ReceivedAmount, row.Decimals),
			"fee_amount":      FormatMinor(row.FeeAmount, row.Decimals),
			"asset":           row.Asset,
			"quote_id":        row.QuoteID,
			"rate":            row.Rate,
			"txid":            row.TxID,
			"state_version":   strconv.FormatUint(row.StateVersion, 10),
			"occurred_at":     formatTime(row.OccurredAt),
		})
	}
	if len(rows) > limit {
		// The cursor is the sort keys of the last returned row (the next page
		// starts strictly after it); the extra row only signals continuation.
		last := rows[limit-1]
		if page.NextCursor, err = encodeCursor("payments", []string{
			last.OccurredAt.UTC().Format(cursorTimeLayout), last.PaymentID, last.EventID,
		}); err != nil {
			return Page{}, err
		}
	}
	return page, nil
}

// queryPayouts returns a page of the payouts report.
func (s *Service) queryPayouts(ctx context.Context, filter storage.ReportFilter, limit int) (Page, error) {
	rows, err := storage.QueryPayouts(ctx, s.db, filter)
	if err != nil {
		return Page{}, err
	}
	page := Page{Rows: make([]map[string]string, 0, min(len(rows), limit))}
	for index, row := range rows {
		if index == limit {
			break
		}
		page.Rows = append(page.Rows, map[string]string{
			"payout_id":         row.PayoutID,
			"merchant_id":       row.MerchantID,
			"event_type":        row.EventType,
			"amount":            FormatMinor(row.Amount, row.Decimals),
			"fee_amount":        FormatMinor(row.FeeAmount, row.Decimals),
			"asset":             row.Asset,
			"txid":              row.TxID,
			"provider":          row.Provider,
			"failure_reason":    row.FailureReason,
			"refund_invoice_id": row.RefundInvoiceID,
			"state_version":     strconv.FormatUint(row.StateVersion, 10),
			"occurred_at":       formatTime(row.OccurredAt),
		})
	}
	if len(rows) > limit {
		last := rows[limit-1]
		if page.NextCursor, err = encodeCursor("payouts", []string{
			last.OccurredAt.UTC().Format(cursorTimeLayout), last.PayoutID, last.EventID,
		}); err != nil {
			return Page{}, err
		}
	}
	return page, nil
}

// queryLedgerEntries returns a page of the ledger_entries report.
func (s *Service) queryLedgerEntries(ctx context.Context, filter storage.ReportFilter, limit int) (Page, error) {
	rows, err := storage.QueryLedgerEntries(ctx, s.db, filter)
	if err != nil {
		return Page{}, err
	}
	page := Page{Rows: make([]map[string]string, 0, min(len(rows), limit))}
	for index, row := range rows {
		if index == limit {
			break
		}
		page.Rows = append(page.Rows, map[string]string{
			"transaction_id": row.TransactionID,
			"entry_index":    strconv.FormatUint(uint64(row.EntryIndex), 10),
			"account_id":     row.AccountID,
			"account_type":   row.AccountType,
			"merchant_id":    row.MerchantID,
			"asset":          row.Asset,
			"direction":      row.Direction,
			"amount":         FormatMinor(row.Amount, row.Decimals),
			"ref_type":       row.RefType,
			"ref_id":         row.RefID,
			"event_type":     row.EventType,
			"occurred_at":    formatTime(row.OccurredAt),
		})
	}
	if len(rows) > limit {
		last := rows[limit-1]
		if page.NextCursor, err = encodeCursor("ledger_entries", []string{
			last.OccurredAt.UTC().Format(cursorTimeLayout), last.TransactionID,
			strconv.FormatUint(uint64(last.EntryIndex), 10), last.EventID,
		}); err != nil {
			return Page{}, err
		}
	}
	return page, nil
}

// queryTurnover returns a page of the merchant turnover aggregate.
func (s *Service) queryTurnover(ctx context.Context, filter storage.ReportFilter, limit int, groupBy string) (Page, error) {
	rows, err := storage.QueryTurnover(ctx, s.db, filter)
	if err != nil {
		return Page{}, err
	}
	page := Page{Rows: make([]map[string]string, 0, min(len(rows), limit))}
	for index, row := range rows {
		if index == limit {
			break
		}
		values := map[string]string{
			"debit_total":   FormatMinor(row.DebitTotal, row.Decimals),
			"credit_total":  FormatMinor(row.CreditTotal, row.Decimals),
			"entries_count": strconv.FormatInt(row.EntriesCount, 10),
		}
		if groupBy == "day_asset" {
			values["day"] = row.Group + "T00:00:00.000Z"
			values["asset"] = row.Asset
		} else {
			values["asset"] = row.Group
		}
		page.Rows = append(page.Rows, values)
	}
	if len(rows) > limit {
		last := rows[limit-1]
		keys := []string{last.Group}
		if groupBy == "day_asset" {
			keys = []string{last.Group, last.Asset}
		}
		if page.NextCursor, err = encodeCursor("turnover", keys); err != nil {
			return Page{}, err
		}
	}
	return page, nil
}
