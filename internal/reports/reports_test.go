package reports

import (
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cbl/cbl-reporting/internal/storage"
)

// canonicalMoneyPattern defines the canonical form of contract money values.
var canonicalMoneyPattern = regexp.MustCompile(`^-?(0|[1-9][0-9]*)(\.[0-9]+)?$`)

// TestFormatMinor checks the canonical decimal string from minor units
// (float is forbidden; sign and zero scale).
func TestFormatMinor(t *testing.T) {
	cases := []struct {
		minor    int64
		decimals uint8
		want     string
	}{
		{0, 0, "0"},
		{0, 6, "0.000000"},
		{1, 6, "0.000001"},
		{100_000000, 6, "100.000000"},
		{-100_000000, 6, "-100.000000"},
		{12345, 2, "123.45"},
		{5, 0, "5"},
		{-5, 0, "-5"},
		{7, 8, "0.00000007"},
		{-1, 18, "-0.000000000000000001"},
		{-9223372036854775808, 0, "-9223372036854775808"},
		{9223372036854775807, 6, "9223372036854.775807"},
	}
	for _, tc := range cases {
		got := FormatMinor(tc.minor, tc.decimals)
		assert.Equal(t, tc.want, got, "FormatMinor(%d, %d)", tc.minor, tc.decimals)
		assert.Regexp(t, canonicalMoneyPattern, got, "canonical value form")
	}
}

// TestCursorRoundTrip checks cursor encoding and decoding.
func TestCursorRoundTrip(t *testing.T) {
	keys := []string{"2026-10-15 10:00:00.000", "pay-1", "ev-1"}
	encoded, err := encodeCursor("payments", keys)
	require.NoError(t, err)
	decoded, err := decodeCursor(encoded, "payments", catalog["payments"].cursorKeys[""])
	require.NoError(t, err)
	assert.Equal(t, keys, decoded)
}

// TestCursorInvalid checks rejection of a broken, foreign and mismatched cursor.
func TestCursorInvalid(t *testing.T) {
	encoded, err := encodeCursor("payments", []string{"2026-10-15 10:00:00.000", "pay-1", "ev-1"})
	require.NoError(t, err)
	kinds := catalog["payments"].cursorKeys[""]

	_, err = decodeCursor("not-base64!!", "payments", kinds)
	require.ErrorIs(t, err, ErrInvalidArgument)

	_, err = decodeCursor(encoded, "payouts", kinds)
	require.ErrorIs(t, err, ErrInvalidArgument, "cursor of another report")

	_, err = decodeCursor(encoded, "payments", catalog["ledger_entries"].cursorKeys[""])
	require.ErrorIs(t, err, ErrInvalidArgument, "mismatched key count")
}

// TestCursorKeyValidation checks rejection of a structurally valid but
// corrupted cursor (keys must not reach ClickHouse).
func TestCursorKeyValidation(t *testing.T) {
	cases := []struct {
		name   string
		report string
		group  string
		keys   []string
	}{
		{"garbage time", "payments", "", []string{"garbage", "pay-1", "ev-1"}},
		{"empty text key", "payments", "", []string{"2026-10-15 10:00:00.000", "", "ev-1"}},
		{"bad index", "ledger_entries", "", []string{"2026-10-15 10:00:00.000", "tx-1", "not-int", "ev-1"}},
		{"index overflow", "ledger_entries", "", []string{"2026-10-15 10:00:00.000", "tx-1", "256", "ev-1"}},
		{"bad day", "turnover", "day_asset", []string{"15.10.2026", "USDT"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			payload, err := json.Marshal(cursorPayload{Version: cursorVersion, Report: tc.report, Keys: tc.keys})
			require.NoError(t, err)
			raw := base64.RawURLEncoding.EncodeToString(payload)
			kinds := catalog[tc.report].cursorKeys[tc.group]
			_, err = decodeCursor(raw, tc.report, kinds)
			require.ErrorIs(t, err, ErrInvalidArgument)
		})
	}
}

// TestCursorTooLong checks rejection of a cursor over the contract length.
func TestCursorTooLong(t *testing.T) {
	db, err := storage.Open("clickhouse://cbl:cbl@127.0.0.1:1/reporting")
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	service, err := New(db)
	require.NoError(t, err)
	_, err = service.Query(t.Context(), QueryRequest{
		ReportType: "payments", Cursor: strings.Repeat("a", maxCursorLength+1),
	})
	require.ErrorIs(t, err, ErrInvalidArgument)
}

// TestValidate checks catalog limits.
func TestValidate(t *testing.T) {
	now := time.Now()
	earlier := now.Add(-time.Hour)

	def := catalog["payments"]
	require.ErrorIs(t, validate(QueryRequest{Limit: 101}, def), ErrInvalidArgument)
	require.ErrorIs(t, validate(QueryRequest{PeriodFrom: &now, PeriodTo: &earlier}, def), ErrInvalidArgument)
	require.NoError(t, validate(QueryRequest{Limit: 100, PeriodFrom: &earlier, PeriodTo: &now}, def))

	turnover := catalog["turnover"]
	require.ErrorIs(t, validate(QueryRequest{}, turnover), ErrInvalidArgument, "turnover requires a merchant")
	require.NoError(t, validate(QueryRequest{MerchantID: "m-1"}, turnover))
}

// TestResolveGroupBy checks the grouping whitelist.
func TestResolveGroupBy(t *testing.T) {
	_, err := resolveGroupBy("asset", catalog["payments"])
	require.ErrorIs(t, err, ErrInvalidArgument, "payments without grouping")

	groupBy, err := resolveGroupBy("", catalog["turnover"])
	require.NoError(t, err)
	assert.Equal(t, "asset", groupBy, "default turnover grouping")

	groupBy, err = resolveGroupBy("day_asset", catalog["turnover"])
	require.NoError(t, err)
	assert.Equal(t, "day_asset", groupBy)

	_, err = resolveGroupBy("day", catalog["turnover"])
	require.ErrorIs(t, err, ErrInvalidArgument)
}

// TestReportsCatalog checks the composition of the closed catalog.
func TestReportsCatalog(t *testing.T) {
	assert.Equal(t, []string{"ledger_entries", "payments", "payouts", "turnover"}, Reports())
}

// TestQueryUnknownReport checks rejection of a type outside the catalog
// (before accessing ClickHouse: the connection is not resolved).
func TestQueryUnknownReport(t *testing.T) {
	db, err := storage.Open("clickhouse://cbl:cbl@127.0.0.1:1/reporting")
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	service, err := New(db)
	require.NoError(t, err)

	_, err = service.Query(t.Context(), QueryRequest{ReportType: "invoices"})
	require.ErrorIs(t, err, ErrUnknownReport)
}

// TestNewRequiresDB checks that the storage is required.
func TestNewRequiresDB(t *testing.T) {
	var db *sql.DB
	_, err := New(db)
	require.Error(t, err)
}

// TestTruncateMilli checks alignment of period bounds to milliseconds.
func TestTruncateMilli(t *testing.T) {
	value := time.Date(2026, 10, 15, 10, 0, 0, 123456789, time.UTC)
	truncated := truncateMilli(&value)
	require.NotNil(t, truncated)
	assert.Equal(t, 123, truncated.Nanosecond()/1e6)
	assert.Nil(t, truncateMilli(nil))
}

// TestFormatTime checks RFC 3339 with milliseconds in UTC.
func TestFormatTime(t *testing.T) {
	value := time.Date(2026, 10, 15, 10, 0, 0, 123456789, time.UTC)
	assert.Equal(t, "2026-10-15T10:00:00.123Z", formatTime(value))
}
