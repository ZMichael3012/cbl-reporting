//go:build integration

package vocabulary

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

	"github.com/cbl/cbl-reporting/internal/domain"
	"github.com/cbl/cbl-reporting/internal/ledger"
	"github.com/cbl/cbl-reporting/internal/metrics"
	"github.com/cbl/cbl-reporting/internal/storage"
	"github.com/cbl/cbl-reporting/test/ledgerstub"
)

// startClickHouse starts ClickHouse on a real container (without Kafka:
// the vocabulary and backfill need storage only).
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

// insertLedgerFact inserts a ledger posting fact row with a time in the
// past (the vocabulary and backfill are not limited by a window).
func insertLedgerFact(t *testing.T, db *sql.DB, transactionID string, entryIndex int, accountID, accountType, merchantID string) {
	t.Helper()
	query := fmt.Sprintf(`INSERT INTO reporting.fact_ledger_entries
(event_id, transaction_id, entry_index, account_id, account_type, merchant_id, asset, decimals,
 direction, amount, ref_type, ref_id, event_type, occurred_at) VALUES
(%s, %s, %d, %s, %s, %s, 'USDT', 6, 'debit', 100000000, 'payment', 'pay-1', 'transaction.posted',
 toDateTime64('2026-10-15 10:00:00.000', 3, 'UTC'))`,
		chQuote(uuid.NewString()), chQuote(transactionID), entryIndex,
		chQuote(accountID), chQuote(accountType), chQuote(merchantID))
	_, err := db.ExecContext(context.Background(), query)
	require.NoError(t, err)
}

// insertPaymentFact inserts a payment fact row (the source of the required
// vocabulary coverage by asset/scale pairs).
func insertPaymentFact(t *testing.T, db *sql.DB, paymentID, merchantID, asset string, decimals uint8) {
	t.Helper()
	query := fmt.Sprintf(`INSERT INTO reporting.fact_payment_events
(event_id, payment_id, merchant_id, event_type, expected_amount, received_amount, fee_amount,
 asset, decimals, quote_id, rate, txid, state_version, occurred_at) VALUES
(%s, %s, %s, 'payment.completed', 100000000, 100000000, 1000000, %s, %d, 'quote-1', '1.0',
 '0xabc', 1, toDateTime64('2026-10-15 10:00:00.000', 3, 'UTC'))`,
		chQuote(uuid.NewString()), chQuote(paymentID), chQuote(merchantID), chQuote(asset), decimals)
	_, err := db.ExecContext(context.Background(), query)
	require.NoError(t, err)
}

// TestVocabularyCycleIntegration checks the vocabulary cycle: filling with
// merchant (Ledger gRPC) and system (derivation) accounts, history backfill,
// idempotency and degradation with an unavailable Ledger.
func TestVocabularyCycleIntegration(t *testing.T) {
	ctx := context.Background()
	db := startClickHouse(t)
	require.NoError(t, storage.Migrate(ctx, db, filepath.Join("..", "..", "migrations")))

	stub := ledgerstub.New()
	stub.SeedAccounts("m-1",
		ledgerstub.Account{ID: "acc-merchant", Type: "merchant", Asset: "USDT", Decimals: 6},
		ledgerstub.Account{ID: "acc-payout", Type: "payout", Asset: "USDT", Decimals: 6},
	)
	stub.SeedAccounts("m-zero") // a merchant without accounts: an empty response is not an error
	addr, err := stub.Start("")
	require.NoError(t, err)
	t.Cleanup(stub.Stop)
	client, err := ledger.NewClient(addr, 5*time.Second)
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	counters := &metrics.Counters{}
	syncer, err := New(db, client, time.Minute, 100, 100, nil, counters)
	require.NoError(t, err)

	// History before the vocabulary: a merchant account, an out-of-plan account and a source payment.
	transactionID := "tx-" + uuid.NewString()
	insertLedgerFact(t, db, transactionID, 0, "acc-merchant", "", "")
	insertLedgerFact(t, db, transactionID, 1, "acc-legacy", "", "")
	insertPaymentFact(t, db, "pay-"+uuid.NewString(), "m-1", "USDT", 6)
	insertPaymentFact(t, db, "pay-"+uuid.NewString(), "m-zero", "USDT", 6)

	inserted, mutations, err := syncer.Cycle(ctx)
	require.NoError(t, err)
	// Vocabulary: two m-1 accounts from the Ledger plus three system ones for
	// USDT/6; m-zero without accounts creates no rows.
	assert.Equal(t, 5, inserted)
	// Backfill: a mutation only for acc-merchant (acc-legacy is outside the vocabulary).
	assert.Equal(t, 1, mutations)
	assert.Equal(t, int64(5), counters.VocabularyAccounts.Load())
	assert.Equal(t, int64(1), counters.BackfillMutations.Load())

	var accountType, merchantID string
	require.NoError(t, db.QueryRowContext(ctx,
		"SELECT account_type, merchant_id FROM reporting.dim_accounts FINAL WHERE account_id = 'acc-merchant'").
		Scan(&accountType, &merchantID))
	assert.Equal(t, "merchant", accountType)
	assert.Equal(t, "m-1", merchantID)

	clearingID, err := domain.DeriveAccountID("clearing", "USDT", 6, "")
	require.NoError(t, err)
	assert.Equal(t, int64(1), scalarCount(t, db,
		"SELECT count() FROM reporting.dim_accounts FINAL WHERE account_id = "+chQuote(clearingID)))

	// Backfill enriched the historical fact row (ClickHouse mutations are
	// asynchronous - waiting with a deadline); the out-of-plan account is untouched.
	waitEnriched(t, db, transactionID, "acc-merchant", "merchant", "m-1")
	require.NoError(t, db.QueryRowContext(ctx, fmt.Sprintf(
		"SELECT account_type FROM reporting.fact_ledger_entries FINAL WHERE transaction_id = %s AND account_id = 'acc-legacy'",
		chQuote(transactionID))).Scan(&accountType))
	assert.Empty(t, accountType, "an out-of-plan account stays without a vocabulary record")

	// A repeated cycle is idempotent.
	inserted, mutations, err = syncer.Cycle(ctx)
	require.NoError(t, err)
	assert.Zero(t, inserted, "a repeated cycle does not duplicate the vocabulary")
	assert.Zero(t, mutations, "enriched rows are not mutated again")

	// A new asset of a known merchant: the payment fact requires a merchant
	// account for the (BTC, 8) pair missing from the vocabulary - the merchant
	// is resynchronized, only the new row is inserted (diff).
	stub.SeedAccounts("m-1",
		ledgerstub.Account{ID: "acc-merchant", Type: "merchant", Asset: "USDT", Decimals: 6},
		ledgerstub.Account{ID: "acc-payout", Type: "payout", Asset: "USDT", Decimals: 6},
		ledgerstub.Account{ID: "acc-merchant-btc", Type: "merchant", Asset: "BTC", Decimals: 8},
	)
	insertPaymentFact(t, db, "pay-"+uuid.NewString(), "m-1", "BTC", 8)
	inserted, mutations, err = syncer.Cycle(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, inserted, "a new account of a known merchant gets into the vocabulary")
	assert.Zero(t, mutations)
	require.NoError(t, db.QueryRowContext(ctx,
		"SELECT account_type FROM reporting.dim_accounts FINAL WHERE account_id = 'acc-merchant-btc'").
		Scan(&accountType))
	assert.Equal(t, "merchant", accountType)

	// An unavailable Ledger does not bring the service down: the cycle returns
	// an error, the error counter grows, the next cycle will retry.
	stub.Stop()
	insertPaymentFact(t, db, "pay-"+uuid.NewString(), "m-2", "USDT", 6)
	_, _, err = syncer.Cycle(ctx)
	require.Error(t, err)
	syncer.runCycle(ctx)
	assert.Equal(t, int64(1), counters.VocabularyErrors.Load())
}

// waitEnriched waits for a fact row to be enriched with vocabulary values
// (ClickHouse mutations are applied asynchronously).
func waitEnriched(t *testing.T, db *sql.DB, transactionID, accountID, wantType, wantMerchant string) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		var accountType, merchantID string
		require.NoError(t, db.QueryRowContext(context.Background(), fmt.Sprintf(
			"SELECT account_type, merchant_id FROM reporting.fact_ledger_entries FINAL WHERE transaction_id = %s AND account_id = %s",
			chQuote(transactionID), chQuote(accountID))).Scan(&accountType, &merchantID))
		if accountType == wantType && merchantID == wantMerchant {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("row is not enriched: account_type=%q merchant_id=%q", accountType, merchantID)
		}
		time.Sleep(time.Second)
	}
}

// scalarCount executes a COUNT query against ClickHouse.
func scalarCount(t *testing.T, db *sql.DB, query string) int64 {
	t.Helper()
	var count int64
	require.NoError(t, db.QueryRowContext(context.Background(), query).Scan(&count))
	return count
}
