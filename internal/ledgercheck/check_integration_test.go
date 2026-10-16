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

// startClickHouse starts ClickHouse in a real container.
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

// chQuote escapes a literal of test INSERTs.
func chQuote(value string) string {
	return "'" + strings.ReplaceAll(strings.ReplaceAll(value, `\`, `\\`), `'`, `\'`) + "'"
}

// insertFact inserts a fact entry with a timestamp inside the reconciliation window.
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

// seedDim inserts a dictionary row.
func seedDim(t *testing.T, db *sql.DB, accountID string) {
	t.Helper()
	require.NoError(t, storage.InsertDimAccounts(context.Background(), db, []storage.DimAccount{{
		AccountID: accountID, AccountType: "merchant", MerchantID: "m-1", Asset: "USDT", Decimals: 6,
	}}))
}

// TestLedgerCheckIntegration verifies reconciliation: a match, all discrepancy
// classes, page truncation and degradation with an unavailable Ledger.
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
	assert.Equal(t, 2, summary.Checked, "matching data and an empty account are not discrepancies")
	assert.Zero(t, summary.Missing+summary.Extra+summary.Mismatch)
	assert.Equal(t, int64(2), counters.CheckAccounts.Load())

	// Controlled discrepancies: an entry only in Ledger, only in the facts, and
	// an amount mismatch.
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

	// Rows outside the window do not participate in reconciliation: an entry older
	// than the window does not produce missing_in_reporting (only "closed" windows
	// are compared).
	outOfWindow := time.Now().UTC().Add(-3 * time.Hour).Truncate(time.Millisecond)
	oldTx := "tx-old-" + uuid.NewString()
	stub.SeedEntries("a1",
		ledgerstub.Entry{TransactionID: matchTx, Direction: "debit", AmountMinor: 100_000000, CreatedAt: inWindow},
		ledgerstub.Entry{TransactionID: oldTx, Direction: "debit", AmountMinor: 1, CreatedAt: outOfWindow},
	)
	stub.SeedEntries("a2")
	summary, err = checker.Cycle(ctx)
	require.NoError(t, err)
	assert.Zero(t, summary.Missing, "entries outside the window are not reconciled")
	assert.Zero(t, summary.Mismatch)

	// Page limit exceeded: the account is skipped with the truncated counter.
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
	assert.Equal(t, 1, summary.Truncated, "pages beyond the limit mean a skip")
	assert.Zero(t, summary.Missing+summary.Mismatch, "a truncated account is not classified")
	assert.Equal(t, int64(1), counters.CheckTruncated.Load())

	// Walk cursor: it advances past processed accounts; a full failure of a portion
	// (Ledger unavailable) leaves the cursor in place - the next cycle repeats the
	// same portion, the cycle does not fail.
	cursored, err := New(db, client, time.Minute, window, 2, 1000, 10, nil, counters)
	require.NoError(t, err)
	first, err := cursored.Cycle(ctx)
	require.NoError(t, err)
	assert.Equal(t, 2, first.Accounts)
	require.NotEmpty(t, cursored.cursor, "the cursor advanced past successful accounts")
	cursorBefore := cursored.cursor
	stub.Stop()
	summary, err = cursored.Cycle(ctx)
	require.NoError(t, err, "account errors do not bring the cycle down")
	assert.Equal(t, 2, summary.Errors)
	assert.Zero(t, summary.Checked)
	assert.Equal(t, cursorBefore, cursored.cursor, "a full failure of the portion does not move the cursor")
	assert.Equal(t, int64(2), counters.CheckErrors.Load())
}

// TestLedgerCheckRoundRobinIntegration verifies the paginated dictionary walk with
// a wrap: all accounts are reconciled in two cycles, the cursor is not lost.
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
	assert.Equal(t, 2, first.Accounts, "the portion per cycle is bounded")
	second, err := checker.Cycle(ctx)
	require.NoError(t, err)
	assert.Equal(t, 2, second.Accounts, "the wrap picks up the remainder and starts a new lap")
	third, err := checker.Cycle(ctx)
	require.NoError(t, err)
	assert.Equal(t, 2, third.Accounts)
	// In three cycles each portion is covered twice and all accounts are walked.
	assert.Equal(t, 6, first.Checked+second.Checked+third.Checked)
}
