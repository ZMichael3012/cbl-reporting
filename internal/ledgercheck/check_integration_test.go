//go:build integration

package ledgercheck

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/cbl/cbl-reporting/internal/ledger"
	"github.com/cbl/cbl-reporting/internal/metrics"
	"github.com/cbl/cbl-reporting/internal/storage"
	"github.com/cbl/cbl-reporting/test/ledgerstub"
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

// insertFact вставляет проводку факта со временем в окне сверки.
func insertFact(t *testing.T, db *sql.DB, transactionID, accountID, direction string, amount int64, occurredAt time.Time) {
	t.Helper()
	query := fmt.Sprintf(`INSERT INTO reporting.fact_ledger_entries
(event_id, transaction_id, entry_index, account_id, account_type, merchant_id, asset, decimals,
 direction, amount, ref_type, ref_id, event_type, occurred_at) VALUES
(%s, %s, 0, %s, 'merchant', 'm-1', 'USDT', 6, %s, %d, 'payment', 'pay-1', 'transaction.posted',
 toDateTime64(%s, 3, 'UTC'))`,
		chQuote(uuid.NewString()), chQuote(transactionID), chQuote(accountID),
		chQuote(direction), amount, chQuote(occurredAt.UTC().Format("2006-01-02 15:04:05.000")))
	_, err := db.ExecContext(context.Background(), query)
	require.NoError(t, err)
}

// seedDim вносит строку словаря.
func seedDim(t *testing.T, db *sql.DB, accountID string) {
	t.Helper()
	require.NoError(t, storage.InsertDimAccounts(context.Background(), db, []storage.DimAccount{{
		AccountID: accountID, AccountType: "merchant", MerchantID: "m-1", Asset: "USDT", Decimals: 6,
	}}))
}

// TestLedgerCheckIntegration проверяет сверку: совпадение, все классы
// расхождений, усечение по страницам и деградацию при недоступном Ledger.
func TestLedgerCheckIntegration(t *testing.T) {
	ctx := context.Background()
	db := startClickHouse(t)
	require.NoError(t, storage.Migrate(ctx, db, filepath.Join("..", "..", "migrations")))

	window := time.Hour
	inWindow := time.Now().UTC().Add(-90 * time.Minute).Truncate(time.Millisecond)

	stub := ledgerstub.New()
	addr, err := stub.Start("")
	require.NoError(t, err)
	t.Cleanup(stub.Stop)
	client, err := ledger.NewClient(addr, 5*time.Second)
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	counters := &metrics.Counters{}
	checker, err := New(db, client, time.Minute, window, 10, 1000, 10, nil, counters)
	require.NoError(t, err)

	matchTx := "tx-match-" + uuid.NewString()
	seedDim(t, db, "a1")
	seedDim(t, db, "a2")
	insertFact(t, db, matchTx, "a1", "debit", 100_000000, inWindow)
	stub.SeedEntries("a1", ledgerstub.Entry{
		TransactionID: matchTx, Direction: "debit", AmountMinor: 100_000000, CreatedAt: inWindow,
	})

	summary, err := checker.Cycle(ctx)
	require.NoError(t, err)
	assert.Equal(t, 2, summary.Accounts)
	assert.Equal(t, 2, summary.Checked, "совпадающие данные и пустой счет - не расхождения")
	assert.Zero(t, summary.Missing+summary.Extra+summary.Mismatch)
	assert.Equal(t, int64(2), counters.CheckAccounts.Load())

	// Управляемые расхождения: проводка только в Ledger, только в фактах и
	// расхождение сумм.
	missingTx := "tx-missing-" + uuid.NewString()
	mismatchTx := "tx-mismatch-" + uuid.NewString()
	extraTx := "tx-extra-" + uuid.NewString()
	stub.SeedEntries("a1",
		ledgerstub.Entry{TransactionID: matchTx, Direction: "debit", AmountMinor: 100_000000, CreatedAt: inWindow},
		ledgerstub.Entry{TransactionID: missingTx, Direction: "credit", AmountMinor: 5, CreatedAt: inWindow},
		ledgerstub.Entry{TransactionID: mismatchTx, Direction: "credit", AmountMinor: 5, CreatedAt: inWindow},
	)
	insertFact(t, db, mismatchTx, "a1", "credit", 7, inWindow)
	insertFact(t, db, extraTx, "a2", "debit", 9, inWindow)

	summary, err = checker.Cycle(ctx)
	require.NoError(t, err)
	assert.Equal(t, 2, summary.Checked)
	assert.Equal(t, 1, summary.Missing, "missing_in_reporting")
	assert.Equal(t, 1, summary.Extra, "extra_in_reporting")
	assert.Equal(t, 1, summary.Mismatch, "amount_mismatch")
	assert.Equal(t, int64(1), counters.CheckMissing.Load())
	assert.Equal(t, int64(1), counters.CheckExtra.Load())
	assert.Equal(t, int64(1), counters.CheckMismatch.Load())

	// Строки за окном не участвуют в сверке: проводка старше окна не дает
	// missing_in_reporting (сравниваются только "закрытые" окна).
	outOfWindow := time.Now().UTC().Add(-3 * time.Hour).Truncate(time.Millisecond)
	oldTx := "tx-old-" + uuid.NewString()
	stub.SeedEntries("a1",
		ledgerstub.Entry{TransactionID: matchTx, Direction: "debit", AmountMinor: 100_000000, CreatedAt: inWindow},
		ledgerstub.Entry{TransactionID: oldTx, Direction: "debit", AmountMinor: 1, CreatedAt: outOfWindow},
	)
	stub.SeedEntries("a2")
	summary, err = checker.Cycle(ctx)
	require.NoError(t, err)
	assert.Zero(t, summary.Missing, "проводки вне окна не сверяются")
	assert.Zero(t, summary.Mismatch)

	// Превышение лимита страниц: счет пропускается со счетчиком truncated.
	truncatedAccount := "a3"
	seedDim(t, db, truncatedAccount)
	var entries []ledgerstub.Entry
	for i := 0; i < 5; i++ {
		entries = append(entries, ledgerstub.Entry{
			TransactionID: fmt.Sprintf("tx-%d-%s", i, uuid.NewString()),
			Direction:     "debit", AmountMinor: 1, CreatedAt: inWindow,
		})
	}
	stub.SeedEntries(truncatedAccount, entries...)
	truncated, err := New(db, client, time.Minute, window, 10, 2, 2, nil, counters)
	require.NoError(t, err)
	summary, err = truncated.Cycle(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, summary.Truncated, "страницы сверх лимита - пропуск")
	assert.Zero(t, summary.Missing+summary.Mismatch, "усеченный счет без классификации")
	assert.Equal(t, int64(1), counters.CheckTruncated.Load())

	// Курсор обхода: продвигается за обработанные счета; полный сбой порции
	// (Ledger недоступен) оставляет курсор на месте - следующий цикл повторит
	// ту же порцию, цикл не падает.
	cursored, err := New(db, client, time.Minute, window, 2, 1000, 10, nil, counters)
	require.NoError(t, err)
	first, err := cursored.Cycle(ctx)
	require.NoError(t, err)
	assert.Equal(t, 2, first.Accounts)
	require.NotEmpty(t, cursored.cursor, "курсор сдвинулся за успешные счета")
	cursorBefore := cursored.cursor
	stub.Stop()
	summary, err = cursored.Cycle(ctx)
	require.NoError(t, err, "ошибки счетов не роняют цикл")
	assert.Equal(t, 2, summary.Errors)
	assert.Zero(t, summary.Checked)
	assert.Equal(t, cursorBefore, cursored.cursor, "полный сбой порции не двигает курсор")
	assert.Equal(t, int64(2), counters.CheckErrors.Load())
}

// TestLedgerCheckRoundRobinIntegration проверяет постраничный обход словаря с
// заворотом: за два цикла сверяются все счета, курсор не теряется.
func TestLedgerCheckRoundRobinIntegration(t *testing.T) {
	ctx := context.Background()
	db := startClickHouse(t)
	require.NoError(t, storage.Migrate(ctx, db, filepath.Join("..", "..", "migrations")))

	stub := ledgerstub.New()
	addr, err := stub.Start("")
	require.NoError(t, err)
	t.Cleanup(stub.Stop)
	client, err := ledger.NewClient(addr, 5*time.Second)
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	checker, err := New(db, client, time.Minute, time.Hour, 2, 1000, 10, nil, nil)
	require.NoError(t, err)
	for _, accountID := range []string{"ra1", "ra2", "ra3"} {
		seedDim(t, db, accountID)
	}

	first, err := checker.Cycle(ctx)
	require.NoError(t, err)
	assert.Equal(t, 2, first.Accounts, "порция за цикл ограничена")
	second, err := checker.Cycle(ctx)
	require.NoError(t, err)
	assert.Equal(t, 2, second.Accounts, "заворот добирает остаток и начинает круг")
	third, err := checker.Cycle(ctx)
	require.NoError(t, err)
	assert.Equal(t, 2, third.Accounts)
	// За три цикла каждая порция покрыта по два раза, все счета пройдены.
	assert.Equal(t, 6, first.Checked+second.Checked+third.Checked)
}
