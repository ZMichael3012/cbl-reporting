//go:build integration

package reports_test

import (
	"context"
	"database/sql"
	"encoding/base64"
	"fmt"
	"net"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	reportingv1 "github.com/ZMichael3012/cbl-protos/gen/go/billing/reporting/v1"

	"github.com/cbl/cbl-reporting/internal/reports"
	"github.com/cbl/cbl-reporting/internal/storage"
	"github.com/cbl/cbl-reporting/internal/transport"
)

// startClickHouse starts ClickHouse on a real container.
func startClickHouse(t *testing.T) *sql.DB {
	t.Helper()
	ctx := context.Background()
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "clickhouse/clickhouse-server:24.8-alpine",
			ExposedPorts: []string{"9000/tcp"},
			Env: map[string]string{
				"CLICKHOUSE_USER":     "cbl",
				"CLICKHOUSE_PASSWORD": "cbl",
				"CLICKHOUSE_DB":       "reporting",
			},
			WaitingFor: wait.ForListeningPort("9000/tcp").WithStartupTimeout(90 * time.Second),
		},
		Started: true,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = container.Terminate(context.Background()) })
	host, err := container.Host(ctx)
	require.NoError(t, err)
	port, err := container.MappedPort(ctx, "9000/tcp")
	require.NoError(t, err)

	db, err := storage.Open(fmt.Sprintf("clickhouse://cbl:cbl@%s:%s/reporting", host, port.Port()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	deadline := time.Now().Add(60 * time.Second)
	for {
		if pingErr := storage.Ping(ctx, db); pingErr == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("clickhouse did not become ready")
		}
		time.Sleep(time.Second)
	}
	return db
}

// chQuote escapes literals of test INSERTs.
func chQuote(value string) string {
	return "'" + strings.ReplaceAll(strings.ReplaceAll(value, `\`, `\\`), `'`, `\'`) + "'"
}

// insertPayment inserts a payment fact with the given time.
func insertPayment(t *testing.T, db *sql.DB, paymentID, merchantID string, occurredAt string) {
	t.Helper()
	query := fmt.Sprintf(`INSERT INTO reporting.fact_payment_events
(event_id, payment_id, merchant_id, event_type, expected_amount, received_amount, fee_amount,
 asset, decimals, quote_id, rate, txid, state_version, occurred_at) VALUES
(%s, %s, %s, 'payment.completed', 100000000, 100000000, 1000000, 'USDT', 6, 'quote-1', '1.0',
 '0xabc', 2, toDateTime64(%s, 3, 'UTC'))`,
		chQuote("ev-"+paymentID), chQuote(paymentID), chQuote(merchantID), chQuote(occurredAt))
	_, err := db.ExecContext(context.Background(), query)
	require.NoError(t, err)
}

// insertPayout inserts a payout fact.
func insertPayout(t *testing.T, db *sql.DB, payoutID, merchantID, occurredAt string) {
	t.Helper()
	query := fmt.Sprintf(`INSERT INTO reporting.fact_payouts
(event_id, payout_id, merchant_id, event_type, amount, fee_amount, asset, decimals, txid,
 provider, failure_reason, refund_invoice_id, state_version, occurred_at) VALUES
(%s, %s, %s, 'payout.completed', 50000000, 500000, 'USDT', 6, '0xdef', 'mock', '', 'inv-1', 3,
 toDateTime64(%s, 3, 'UTC'))`,
		chQuote("ev-"+payoutID), chQuote(payoutID), chQuote(merchantID), chQuote(occurredAt))
	_, err := db.ExecContext(context.Background(), query)
	require.NoError(t, err)
}

// insertLedgerEntry inserts a ledger entry (with or without dictionary enrichment).
func insertLedgerEntry(t *testing.T, db *sql.DB, transactionID string, entryIndex int, accountID, accountType, merchantID, asset string, decimals uint8, amount int64, direction, occurredAt string) {
	t.Helper()
	query := fmt.Sprintf(`INSERT INTO reporting.fact_ledger_entries
(event_id, transaction_id, entry_index, account_id, account_type, merchant_id, asset, decimals,
 direction, amount, ref_type, ref_id, event_type, occurred_at) VALUES
(%s, %s, %d, %s, %s, %s, %s, %d, %s, %d, 'payment', 'pay-1', 'transaction.posted',
 toDateTime64(%s, 3, 'UTC'))`,
		chQuote(fmt.Sprintf("ev-%s-%d", transactionID, entryIndex)), chQuote(transactionID), entryIndex,
		chQuote(accountID), chQuote(accountType), chQuote(merchantID), chQuote(asset), decimals,
		chQuote(direction), amount, chQuote(occurredAt))
	_, err := db.ExecContext(context.Background(), query)
	require.NoError(t, err)
}

// seedFacts populates facts for report integration checks.
func seedFacts(t *testing.T, db *sql.DB) {
	t.Helper()
	insertPayment(t, db, "pay-1", "m-1", "2026-10-15 10:01:00.000")
	insertPayment(t, db, "pay-2", "m-1", "2026-10-15 10:02:00.000")
	insertPayment(t, db, "pay-3", "m-2", "2026-10-15 10:03:00.000")
	insertPayout(t, db, "po-1", "m-1", "2026-10-15 10:04:00.000")
	insertPayout(t, db, "po-2", "m-1", "2026-10-15 10:05:00.000")
	insertLedgerEntry(t, db, "tx-1", 0, "acc-merchant", "merchant", "m-1", "USDT", 6, 100_000000, "credit", "2026-10-15 10:01:00.000")
	insertLedgerEntry(t, db, "tx-1", 1, "acc-clearing", "", "", "USDT", 6, 100_000000, "debit", "2026-10-15 10:01:00.000")
	insertLedgerEntry(t, db, "tx-2", 0, "acc-merchant", "merchant", "m-1", "USDT", 6, 50_000000, "credit", "2026-10-14 09:00:00.000")
	insertLedgerEntry(t, db, "tx-3", 0, "acc-merchant", "merchant", "m-1", "ETH", 6, 7_000000, "credit", "2026-10-15 10:07:00.000")
	insertLedgerEntry(t, db, "tx-4", 0, "acc-merchant", "merchant", "m-1", "USDT", 6, -2_000000, "debit", "2026-10-15 10:08:00.000")
}

// TestReportsQueryIntegration checks all catalog reports on a real
// ClickHouse: queries, value format, merchant filter, period and pagination.
func TestReportsQueryIntegration(t *testing.T) {
	ctx := context.Background()
	db := startClickHouse(t)
	require.NoError(t, storage.Migrate(ctx, db, filepath.Join("..", "..", "migrations")))
	seedFacts(t, db)

	service, err := reports.New(db)
	require.NoError(t, err)

	// payments: page, order, canonical money and time.
	page, err := service.Query(ctx, reports.QueryRequest{ReportType: "payments", MerchantID: "m-1", Limit: 20})
	require.NoError(t, err)
	require.Len(t, page.Rows, 2)
	assert.Empty(t, page.NextCursor)
	assert.Equal(t, "pay-1", page.Rows[0]["payment_id"])
	assert.Equal(t, "100.000000", page.Rows[0]["expected_amount"])
	assert.Equal(t, "1.000000", page.Rows[0]["fee_amount"])
	assert.Equal(t, "USDT", page.Rows[0]["asset"])
	assert.Equal(t, "payment.completed", page.Rows[0]["event_type"])
	assert.Equal(t, "2", page.Rows[0]["state_version"])
	assert.Equal(t, "2026-10-15T10:01:00.000Z", page.Rows[0]["occurred_at"])
	assert.Equal(t, "m-1", page.Rows[0]["merchant_id"])

	// Pagination: limit=1, continuation by cursor, end of report.
	first, err := service.Query(ctx, reports.QueryRequest{ReportType: "payments", MerchantID: "m-1", Limit: 1})
	require.NoError(t, err)
	require.Len(t, first.Rows, 1)
	require.NotEmpty(t, first.NextCursor)
	second, err := service.Query(ctx, reports.QueryRequest{ReportType: "payments", MerchantID: "m-1", Limit: 1, Cursor: first.NextCursor})
	require.NoError(t, err)
	require.Len(t, second.Rows, 1)
	assert.Equal(t, "pay-2", second.Rows[0]["payment_id"])
	assert.Empty(t, second.NextCursor)

	// The period is inclusive on both bounds.
	boundary := time.Date(2026, 10, 15, 10, 1, 0, 0, time.UTC)
	exact, err := service.Query(ctx, reports.QueryRequest{
		ReportType: "payments", MerchantID: "m-1", PeriodFrom: &boundary, PeriodTo: &boundary, Limit: 20,
	})
	require.NoError(t, err)
	require.Len(t, exact.Rows, 1)
	assert.Equal(t, "pay-1", exact.Rows[0]["payment_id"])

	// payouts: rows and fields of contract cf-2.1.
	page, err = service.Query(ctx, reports.QueryRequest{ReportType: "payouts", MerchantID: "m-1", Limit: 20})
	require.NoError(t, err)
	require.Len(t, page.Rows, 2)
	assert.Equal(t, "po-1", page.Rows[0]["payout_id"])
	assert.Equal(t, "50.000000", page.Rows[0]["amount"])
	assert.Equal(t, "inv-1", page.Rows[0]["refund_invoice_id"])
	assert.Equal(t, "mock", page.Rows[0]["provider"])

	// ledger_entries: merchant filter by denormalization; rows without a
	// dictionary record (acc-clearing) are invisible to the merchant.
	page, err = service.Query(ctx, reports.QueryRequest{ReportType: "ledger_entries", MerchantID: "m-1", Limit: 20})
	require.NoError(t, err)
	require.Len(t, page.Rows, 4)
	assert.Equal(t, "tx-2", page.Rows[0]["transaction_id"], "order by occurred_at")
	assert.Equal(t, "merchant", page.Rows[0]["account_type"])
	assert.Equal(t, "0", page.Rows[0]["entry_index"])
	assert.Equal(t, "credit", page.Rows[0]["direction"])
	assert.Equal(t, "50.000000", page.Rows[0]["amount"])

	// turnover: aggregates per asset (scales are not mixed), sign of negative sums.
	page, err = service.Query(ctx, reports.QueryRequest{ReportType: "turnover", MerchantID: "m-1", Limit: 20})
	require.NoError(t, err)
	require.Len(t, page.Rows, 2)
	assert.Equal(t, "ETH", page.Rows[0]["asset"])
	assert.Equal(t, "7.000000", page.Rows[0]["credit_total"])
	assert.Equal(t, "1", page.Rows[0]["entries_count"])
	assert.Equal(t, "USDT", page.Rows[1]["asset"])
	assert.Equal(t, "-2.000000", page.Rows[1]["debit_total"], "a negative reversal is formatted with its sign")
	assert.Equal(t, "150.000000", page.Rows[1]["credit_total"])
	assert.Equal(t, "3", page.Rows[1]["entries_count"])

	page, err = service.Query(ctx, reports.QueryRequest{
		ReportType: "turnover", MerchantID: "m-1", GroupBy: "day_asset", Limit: 20,
	})
	require.NoError(t, err)
	require.Len(t, page.Rows, 3)
	assert.Equal(t, "2026-10-14T00:00:00.000Z", page.Rows[0]["day"], "the day is RFC 3339 in UTC")
	assert.Equal(t, "50.000000", page.Rows[0]["credit_total"])
	assert.Equal(t, "2026-10-15T00:00:00.000Z", page.Rows[1]["day"])
	assert.Equal(t, "ETH", page.Rows[1]["asset"])
	assert.Equal(t, "-2.000000", page.Rows[2]["debit_total"])

	// Empty page: a merchant without data has no rows.
	page, err = service.Query(ctx, reports.QueryRequest{ReportType: "payments", MerchantID: "m-empty", Limit: 20})
	require.NoError(t, err)
	assert.Empty(t, page.Rows)
	assert.Empty(t, page.NextCursor)

	// Page-by-page pagination for every report: no duplicates and no losses.
	walkPages(t, service, reports.QueryRequest{ReportType: "payments", MerchantID: "m-1"}, 2)
	walkPages(t, service, reports.QueryRequest{ReportType: "payouts", MerchantID: "m-1"}, 2)
	walkPages(t, service, reports.QueryRequest{ReportType: "ledger_entries", MerchantID: "m-1"}, 4)
	walkPages(t, service, reports.QueryRequest{ReportType: "turnover", MerchantID: "m-1"}, 2)
	walkPages(t, service, reports.QueryRequest{ReportType: "turnover", MerchantID: "m-1", GroupBy: "day_asset"}, 3)
	// Catalog and parameter errors.
	_, err = service.Query(ctx, reports.QueryRequest{ReportType: "invoices"})
	require.ErrorIs(t, err, reports.ErrUnknownReport)
	_, err = service.Query(ctx, reports.QueryRequest{ReportType: "payments", GroupBy: "asset"})
	require.ErrorIs(t, err, reports.ErrInvalidArgument)
	_, err = service.Query(ctx, reports.QueryRequest{ReportType: "turnover"})
	require.ErrorIs(t, err, reports.ErrInvalidArgument)
	_, err = service.Query(ctx, reports.QueryRequest{ReportType: "payments", Limit: 101})
	require.ErrorIs(t, err, reports.ErrInvalidArgument)
	_, err = service.Query(ctx, reports.QueryRequest{ReportType: "payments", Cursor: "not-a-cursor"})
	require.ErrorIs(t, err, reports.ErrInvalidArgument)

	// A structurally valid but corrupted cursor is rejected before ClickHouse.
	forged := base64.RawURLEncoding.EncodeToString([]byte(`{"v":1,"report":"payments","keys":["garbage","x","y"]}`))
	_, err = service.Query(ctx, reports.QueryRequest{ReportType: "payments", Cursor: forged})
	require.ErrorIs(t, err, reports.ErrInvalidArgument, "a corrupted cursor does not reach ClickHouse")
}

// walkPages walks a report page by page with one row per page and checks the
// absence of duplicates, the completeness of the walk (wantRows rows) and
// the cursor termination.
func walkPages(t *testing.T, service *reports.Service, base reports.QueryRequest, wantRows int) {
	t.Helper()
	seen := make(map[string]struct{})
	cursor := ""
	for pageNumber := 0; pageNumber < 20; pageNumber++ {
		request := base
		request.Limit = 1
		request.Cursor = cursor
		page, err := service.Query(context.Background(), request)
		require.NoError(t, err, "page %d", pageNumber)
		for _, row := range page.Rows {
			key := rowKey(row)
			_, duplicate := seen[key]
			require.False(t, duplicate, "duplicate row %q", key)
			seen[key] = struct{}{}
		}
		if page.NextCursor == "" {
			require.Len(t, seen, wantRows, "the walk of %s collected all rows", base.ReportType)
			return
		}
		cursor = page.NextCursor
	}
	t.Fatal("pagination did not finish within 20 pages")
}

// rowKey builds a deterministic uniqueness key of a report row.
func rowKey(row map[string]string) string {
	fields := make([]string, 0, len(row))
	for name := range row {
		fields = append(fields, name)
	}
	sort.Strings(fields)
	var builder strings.Builder
	for _, name := range fields {
		builder.WriteString(name)
		builder.WriteString("=")
		builder.WriteString(row[name])
		builder.WriteString("|")
	}
	return builder.String()
}

// TestReportsGRPCIntegration checks the mapping of domain errors to gRPC
// codes on a real server.
func TestReportsGRPCIntegration(t *testing.T) {
	ctx := context.Background()
	db := startClickHouse(t)
	require.NoError(t, storage.Migrate(ctx, db, filepath.Join("..", "..", "migrations")))
	seedFacts(t, db)

	service, err := reports.New(db)
	require.NoError(t, err)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	server := grpc.NewServer()
	reportingv1.RegisterReportingServiceServer(server, transport.NewReportsServer(service, nil))
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)

	conn, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	client := reportingv1.NewReportingServiceClient(conn)

	response, err := client.Query(ctx, &reportingv1.QueryRequest{ReportType: "payments", MerchantId: "m-1", Limit: 5})
	require.NoError(t, err)
	require.Len(t, response.GetRows(), 2)
	assert.Equal(t, "100.000000", response.GetRows()[0].GetValues()["expected_amount"])

	_, err = client.Query(ctx, &reportingv1.QueryRequest{ReportType: "invoices"})
	assert.Equal(t, codes.NotFound, status.Code(err))

	_, err = client.Query(ctx, &reportingv1.QueryRequest{ReportType: "payments", GroupBy: "asset"})
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
}
