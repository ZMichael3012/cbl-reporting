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

	reportingv1 "github.com/cbl/cbl-protos/gen/go/billing/reporting/v1"

	"github.com/cbl/cbl-reporting/internal/reports"
	"github.com/cbl/cbl-reporting/internal/storage"
	"github.com/cbl/cbl-reporting/internal/transport"
)

// startClickHouse поднимает ClickHouse на реальном контейнере.
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

// chQuote экранирует литерал тестовых INSERT-ов.
func chQuote(value string) string {
	return "'" + strings.ReplaceAll(strings.ReplaceAll(value, `\`, `\\`), `'`, `\'`) + "'"
}

// insertPayment вставляет факт платежа с заданным временем.
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

// insertPayout вставляет факт выплаты.
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

// insertLedgerEntry вставляет проводку леджера (с обогащением словаря или без).
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

// seedFacts наполняет факты для интеграционных проверок отчетов.
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

// TestReportsQueryIntegration проверяет все отчеты каталога на реальном
// ClickHouse: выборки, формат значений, фильтр мерчанта, период и пагинацию.
func TestReportsQueryIntegration(t *testing.T) {
	ctx := context.Background()
	db := startClickHouse(t)
	require.NoError(t, storage.Migrate(ctx, db, filepath.Join("..", "..", "migrations")))
	seedFacts(t, db)

	service, err := reports.New(db)
	require.NoError(t, err)

	// payments: страница, порядок, канонические деньги и время.
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

	// Пагинация: limit=1, продолжение по курсору, конец отчета.
	first, err := service.Query(ctx, reports.QueryRequest{ReportType: "payments", MerchantID: "m-1", Limit: 1})
	require.NoError(t, err)
	require.Len(t, first.Rows, 1)
	require.NotEmpty(t, first.NextCursor)
	second, err := service.Query(ctx, reports.QueryRequest{ReportType: "payments", MerchantID: "m-1", Limit: 1, Cursor: first.NextCursor})
	require.NoError(t, err)
	require.Len(t, second.Rows, 1)
	assert.Equal(t, "pay-2", second.Rows[0]["payment_id"])
	assert.Empty(t, second.NextCursor)

	// Период включается на обеих границах.
	boundary := time.Date(2026, 10, 15, 10, 1, 0, 0, time.UTC)
	exact, err := service.Query(ctx, reports.QueryRequest{
		ReportType: "payments", MerchantID: "m-1", PeriodFrom: &boundary, PeriodTo: &boundary, Limit: 20,
	})
	require.NoError(t, err)
	require.Len(t, exact.Rows, 1)
	assert.Equal(t, "pay-1", exact.Rows[0]["payment_id"])

	// payouts: строки и поля контракта cf-2.1.
	page, err = service.Query(ctx, reports.QueryRequest{ReportType: "payouts", MerchantID: "m-1", Limit: 20})
	require.NoError(t, err)
	require.Len(t, page.Rows, 2)
	assert.Equal(t, "po-1", page.Rows[0]["payout_id"])
	assert.Equal(t, "50.000000", page.Rows[0]["amount"])
	assert.Equal(t, "inv-1", page.Rows[0]["refund_invoice_id"])
	assert.Equal(t, "mock", page.Rows[0]["provider"])

	// ledger_entries: мерчантский фильтр по денормализации; строки без словаря
	// (acc-clearing) мерчанту не видны.
	page, err = service.Query(ctx, reports.QueryRequest{ReportType: "ledger_entries", MerchantID: "m-1", Limit: 20})
	require.NoError(t, err)
	require.Len(t, page.Rows, 4)
	assert.Equal(t, "tx-2", page.Rows[0]["transaction_id"], "порядок по occurred_at")
	assert.Equal(t, "merchant", page.Rows[0]["account_type"])
	assert.Equal(t, "0", page.Rows[0]["entry_index"])
	assert.Equal(t, "credit", page.Rows[0]["direction"])
	assert.Equal(t, "50.000000", page.Rows[0]["amount"])

	// turnover: агрегаты по активу (шкалы не смешиваются), знак отрицательных сумм.
	page, err = service.Query(ctx, reports.QueryRequest{ReportType: "turnover", MerchantID: "m-1", Limit: 20})
	require.NoError(t, err)
	require.Len(t, page.Rows, 2)
	assert.Equal(t, "ETH", page.Rows[0]["asset"])
	assert.Equal(t, "7.000000", page.Rows[0]["credit_total"])
	assert.Equal(t, "1", page.Rows[0]["entries_count"])
	assert.Equal(t, "USDT", page.Rows[1]["asset"])
	assert.Equal(t, "-2.000000", page.Rows[1]["debit_total"], "отрицательное сторно форматируется со знаком")
	assert.Equal(t, "150.000000", page.Rows[1]["credit_total"])
	assert.Equal(t, "3", page.Rows[1]["entries_count"])

	page, err = service.Query(ctx, reports.QueryRequest{
		ReportType: "turnover", MerchantID: "m-1", GroupBy: "day_asset", Limit: 20,
	})
	require.NoError(t, err)
	require.Len(t, page.Rows, 3)
	assert.Equal(t, "2026-10-14T00:00:00.000Z", page.Rows[0]["day"], "день - RFC 3339 в UTC")
	assert.Equal(t, "50.000000", page.Rows[0]["credit_total"])
	assert.Equal(t, "2026-10-15T00:00:00.000Z", page.Rows[1]["day"])
	assert.Equal(t, "ETH", page.Rows[1]["asset"])
	assert.Equal(t, "-2.000000", page.Rows[2]["debit_total"])

	// Пустая страница: у мерчанта без данных строк нет.
	page, err = service.Query(ctx, reports.QueryRequest{ReportType: "payments", MerchantID: "m-empty", Limit: 20})
	require.NoError(t, err)
	assert.Empty(t, page.Rows)
	assert.Empty(t, page.NextCursor)

	// Пагинация постранично для каждого отчета: без дублей и потерь.
	walkPages(t, service, reports.QueryRequest{ReportType: "payments", MerchantID: "m-1"}, 2)
	walkPages(t, service, reports.QueryRequest{ReportType: "payouts", MerchantID: "m-1"}, 2)
	walkPages(t, service, reports.QueryRequest{ReportType: "ledger_entries", MerchantID: "m-1"}, 4)
	walkPages(t, service, reports.QueryRequest{ReportType: "turnover", MerchantID: "m-1"}, 2)
	walkPages(t, service, reports.QueryRequest{ReportType: "turnover", MerchantID: "m-1", GroupBy: "day_asset"}, 3)
	// Ошибки каталога и параметров.
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

	// Структурно корректный, но испорченный курсор отбивается до ClickHouse.
	forged := base64.RawURLEncoding.EncodeToString([]byte(`{"v":1,"report":"payments","keys":["garbage","x","y"]}`))
	_, err = service.Query(ctx, reports.QueryRequest{ReportType: "payments", Cursor: forged})
	require.ErrorIs(t, err, reports.ErrInvalidArgument, "испорченный курсор не доходит до ClickHouse")
}

// walkPages проходит отчет страницами по одной строке и проверяет отсутствие
// дублей, полноту обхода (wantRows строк) и завершение курсора.
func walkPages(t *testing.T, service *reports.Service, base reports.QueryRequest, wantRows int) {
	t.Helper()
	seen := make(map[string]struct{})
	cursor := ""
	for pageNumber := 0; pageNumber < 20; pageNumber++ {
		request := base
		request.Limit = 1
		request.Cursor = cursor
		page, err := service.Query(context.Background(), request)
		require.NoError(t, err, "страница %d", pageNumber)
		for _, row := range page.Rows {
			key := rowKey(row)
			_, duplicate := seen[key]
			require.False(t, duplicate, "дубль строки %q", key)
			seen[key] = struct{}{}
		}
		if page.NextCursor == "" {
			require.Len(t, seen, wantRows, "обход %s собрал все строки", base.ReportType)
			return
		}
		cursor = page.NextCursor
	}
	t.Fatal("пагинация не завершилась за 20 страниц")
}

// rowKey строит детерминированный ключ уникальности строки отчета.
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

// TestReportsGRPCIntegration проверяет маппинг доменных ошибок в коды gRPC на
// реальном сервере.
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
