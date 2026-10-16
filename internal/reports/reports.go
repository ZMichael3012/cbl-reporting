// Пакет reports реализует каталог отчетов ReportingService: валидацию,
// keyset-пагинацию, выборки из ClickHouse и форматирование значений (CBL-11.4).
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

// Доменные ошибки сервиса (транспорт переводит их в коды gRPC).
var (
	// ErrUnknownReport означает тип отчета вне закрытого каталога.
	ErrUnknownReport = errors.New("unknown report type")
	// ErrInvalidArgument означает недопустимые параметры запроса.
	ErrInvalidArgument = errors.New("invalid argument")
)

// Ограничения контракта (billing/reporting/v1) и транспорта.
const (
	defaultLimit    = 20
	maxLimit        = 100
	maxCursorLength = 512
	cursorVersion   = 1
)

// cursorTimeLayout задает формат времени в ключах курсора (точность колонок
// DateTime64(3)).
const cursorTimeLayout = "2006-01-02 15:04:05.000"

// cursorDayLayout задает формат дня в ключах курсора группировки day_asset.
const cursorDayLayout = "2006-01-02"

// cursorKeyKind задает проверяемый формат ключа курсора.
type cursorKeyKind int

const (
	// keyTime задает время в формате курсора фактов.
	keyTime cursorKeyKind = iota
	// keyDay задает день в формате 2006-01-02.
	keyDay
	// keyIndex задает индекс проводки (UInt8).
	keyIndex
	// keyText задает строковый ключ (идентификатор, актив).
	keyText
)

// reportDef описывает запись каталога: группировку, требование мерчанта и
// форматы ключей курсора по вариантам group_by ("" - тип без группировки).
type reportDef struct {
	groupBy          []string
	defaultGroupBy   string
	requiresMerchant bool
	cursorKeys       map[string][]cursorKeyKind
}

// catalog задает закрытый каталог отчетов 11.4.
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

// Reports возвращает типы каталога по алфавиту (документация и диагностика).
func Reports() []string {
	names := make([]string, 0, len(catalog))
	for name := range catalog {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// QueryRequest описывает параметры отчета; период приводится к миллисекундам
// (точность DateTime64(3)).
type QueryRequest struct {
	// ReportType задает тип отчета из каталога.
	ReportType string
	// MerchantID задает мерчанта (пусто - все мерчанты для списков).
	MerchantID string
	// PeriodFrom задает нижнюю границу периода включительно.
	PeriodFrom *time.Time
	// PeriodTo задает верхнюю границу периода включительно.
	PeriodTo *time.Time
	// GroupBy задает измерение группировки (turnover).
	GroupBy string
	// Limit задает размер страницы (0 - дефолт 20, максимум 100).
	Limit uint32
	// Cursor задает непрозрачный курсор продолжения.
	Cursor string
}

// Page описывает страницу отчета.
type Page struct {
	// Rows задает строки страницы (значения - строки).
	Rows []map[string]string
	// NextCursor задает курсор следующей страницы (пусто - конец).
	NextCursor string
}

// Service выполняет запросы отчетов по каталогу.
type Service struct {
	db *sql.DB
}

// New собирает сервис отчетов.
func New(db *sql.DB) (*Service, error) {
	if db == nil {
		return nil, errors.New("clickhouse db is required")
	}
	return &Service{db: db}, nil
}

// Query проверяет параметры, выполняет выборку и возвращает страницу отчета.
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

// validate проверяет параметры запроса против записи каталога.
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

// resolveGroupBy проверяет группировку через whitelist каталога.
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

// truncateMilli приводит границу периода к точности колонок DateTime64(3).
func truncateMilli(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	truncated := value.UTC().Truncate(time.Millisecond)
	return &truncated
}

// cursorPayload описывает непрозрачный курсор (keyset по ключам сортировки).
type cursorPayload struct {
	Version int      `json:"v"`
	Report  string   `json:"report"`
	Keys    []string `json:"keys"`
}

// encodeCursor собирает курсор по ключам сортировки последней выданной строки.
func encodeCursor(report string, keys []string) (string, error) {
	payload, err := json.Marshal(cursorPayload{Version: cursorVersion, Report: report, Keys: keys})
	if err != nil {
		return "", fmt.Errorf("encode cursor: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(payload), nil
}

// decodeCursor разбирает и валидирует курсор: структуру (версия, отчет, число
// ключей) и формат каждого ключа (время, день, индекс, строковый идентификатор).
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

// validateCursorKey проверяет формат одного ключа курсора: структурно
// корректный, но испорченный курсор не должен доходить до ClickHouse.
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

// FormatMinor конвертирует сумму в минимальных единицах в каноническую
// десятичную строку ^-?(0|[1-9][0-9]*)(\.[0-9]+)?$ (float запрещен).
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

// formatTime переводит время в RFC 3339 UTC с миллисекундами.
func formatTime(value time.Time) string {
	return value.UTC().Format("2006-01-02T15:04:05.000Z07:00")
}

// queryPayments отдает страницу отчета payments.
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
		// Курсор - ключи последней выданной строки (следующая страница строго
		// после нее); лишняя строка лишь сигнализирует о продолжении.
		last := rows[limit-1]
		if page.NextCursor, err = encodeCursor("payments", []string{
			last.OccurredAt.UTC().Format(cursorTimeLayout), last.PaymentID, last.EventID,
		}); err != nil {
			return Page{}, err
		}
	}
	return page, nil
}

// queryPayouts отдает страницу отчета payouts.
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

// queryLedgerEntries отдает страницу отчета ledger_entries.
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

// queryTurnover отдает страницу агрегата оборота мерчанта.
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
