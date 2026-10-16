//go:build integration

package storage_test

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	dockertypes "github.com/docker/docker/api/types/container"
	"github.com/docker/go-connections/nat"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/cbl/cbl-reporting/internal/storage"
	"github.com/cbl/cbl-reporting/test/producer"
)

// startNetwork creates an isolated stand network: Kafka is reachable by
// container name for ClickHouse (akin to the service compose network).
func startNetwork(t *testing.T) string {
	t.Helper()
	name := "cbl-reporting-it-" + uuid.NewString()[:8]
	network, err := testcontainers.GenericNetwork(context.Background(), testcontainers.GenericNetworkRequest{
		NetworkRequest: testcontainers.NetworkRequest{Name: name},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = network.Remove(context.Background()) })
	return name
}

// freePort picks a free host port for the external Kafka listener.
func freePort(t *testing.T) int {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer func() { _ = listener.Close() }()
	return listener.Addr().(*net.TCPAddr).Port
}

// startKafka starts Kafka (KRaft, two listeners) in the given network,
// creates the pipeline topics and returns the addresses for ClickHouse
// (internal) and for the test producer (external).
func startKafka(t *testing.T, network string) (internal []string, external []string) {
	t.Helper()
	ctx := context.Background()
	hostPort := freePort(t)
	// The external listener address is known before the start: the host port
	// is fixed by binding (akin to the service compose stand).
	externalBrokers := []string{fmt.Sprintf("localhost:%d", hostPort)}
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "apache/kafka:3.7.0",
			ExposedPorts: []string{"9092/tcp"},
			Networks:     []string{network},
			NetworkAliases: map[string][]string{
				network: {"kafka"},
			},
			Env: map[string]string{
				"KAFKA_NODE_ID":                                  "1",
				"KAFKA_PROCESS_ROLES":                            "broker,controller",
				"KAFKA_CONTROLLER_QUORUM_VOTERS":                 "1@kafka:9093",
				"KAFKA_LISTENERS":                                "PLAINTEXT://0.0.0.0:29092,CONTROLLER://0.0.0.0:9093,EXTERNAL://0.0.0.0:9092",
				"KAFKA_ADVERTISED_LISTENERS":                     "PLAINTEXT://kafka:29092,EXTERNAL://localhost:" + strconv.Itoa(hostPort),
				"KAFKA_LISTENER_SECURITY_PROTOCOL_MAP":           "PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT,EXTERNAL:PLAINTEXT",
				"KAFKA_INTER_BROKER_LISTENER_NAME":               "PLAINTEXT",
				"KAFKA_CONTROLLER_LISTENER_NAMES":                "CONTROLLER",
				"KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR":         "1",
				"KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR": "1",
				"KAFKA_TRANSACTION_STATE_LOG_MIN_ISR":            "1",
				"KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS":         "0",
				"KAFKA_AUTO_CREATE_TOPICS_ENABLE":                "false",
				"CLUSTER_ID":                                     "4L6g3nShT-eMCtK--X86sw",
			},
			HostConfigModifier: func(hostConfig *dockertypes.HostConfig) {
				hostConfig.PortBindings = nat.PortMap{
					nat.Port("9092/tcp"): []nat.PortBinding{{HostIP: "127.0.0.1", HostPort: strconv.Itoa(hostPort)}},
				}
			},
			WaitingFor: wait.ForLog("Kafka Server started").WithStartupTimeout(120 * time.Second),
		},
		Started: true,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = container.Terminate(context.Background()) })

	for _, topic := range []string{"payment.events", "ledger.events", "payout.events", "recon.alerts"} {
		exitCode, _, execErr := container.Exec(ctx, []string{
			"/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server", "kafka:29092",
			"--create", "--topic", topic, "--partitions", "1", "--replication-factor", "1",
		})
		require.NoError(t, execErr)
		require.Zero(t, exitCode, "create topic %s", topic)
	}
	return []string{"kafka:29092"}, externalBrokers
}

// startClickHouse starts ClickHouse in the stand network, copies the wire
// schema and the format_schema_path configuration, returns the service
// connection.
func startClickHouse(t *testing.T, network string) *sql.DB {
	t.Helper()
	ctx := context.Background()
	schemaHostPath, err := filepath.Abs(filepath.Join("..", "..", "deploy", "format_schemas", "events.proto"))
	require.NoError(t, err)
	configHostPath, err := filepath.Abs(filepath.Join("..", "..", "deploy", "clickhouse-config", "10-format-schema-path.xml"))
	require.NoError(t, err)
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "clickhouse/clickhouse-server:24.8-alpine",
			ExposedPorts: []string{"9000/tcp"},
			Networks:     []string{network},
			Env: map[string]string{
				"CLICKHOUSE_USER":     "cbl",
				"CLICKHOUSE_PASSWORD": "cbl",
				"CLICKHOUSE_DB":       "reporting",
			},
			Files: []testcontainers.ContainerFile{
				{
					HostFilePath:      schemaHostPath,
					ContainerFilePath: "/schemas/events.proto",
					FileMode:          0o644,
				},
				{
					HostFilePath:      configHostPath,
					ContainerFilePath: "/etc/clickhouse-server/config.d/10-format-schema-path.xml",
					FileMode:          0o644,
				},
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

	dsn := fmt.Sprintf("clickhouse://cbl:cbl@%s:%s/reporting", host, port.Port())
	db, err := storage.Open(dsn)
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

// scalarCount runs a COUNT query against ClickHouse.
func scalarCount(t *testing.T, db *sql.DB, query string) int64 {
	t.Helper()
	var count int64
	require.NoError(t, db.QueryRowContext(context.Background(), query).Scan(&count))
	return count
}

// waitForCount waits for the expected number of rows (the pipeline is asynchronous).
func waitForCount(t *testing.T, db *sql.DB, query string, expected int64) {
	t.Helper()
	deadline := time.Now().Add(90 * time.Second)
	var actual int64
	for {
		actual = scalarCount(t, db, query)
		if actual == expected {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("the pipeline did not deliver the rows: expected %d, got %d (%s)", expected, actual, query)
		}
		time.Sleep(time.Second)
	}
}

// TestIngestionPipelineIntegration checks the full pipeline: migrations,
// idempotent reconciliation, delivery of events from four topics, values
// and deduplication, resilience to a broken message.
func TestIngestionPipelineIntegration(t *testing.T) {
	ctx := context.Background()
	network := startNetwork(t)
	internalBrokers, externalBrokers := startKafka(t, network)
	db := startClickHouse(t, network)

	require.NoError(t, storage.Migrate(ctx, db, filepath.Join("..", "..", "migrations")))
	migrationCount := scalarCount(t, db, "SELECT count() FROM reporting.goose_db_version")
	require.GreaterOrEqual(t, migrationCount, int64(1), "goose versions are applied")

	pipeline, err := storage.NewPipeline(db, internalBrokers[0], "events.proto", nil)
	require.NoError(t, err)
	require.NoError(t, pipeline.Reconcile(ctx))
	require.NoError(t, pipeline.Reconcile(ctx), "repeated reconciliation is idempotent")
	require.NoError(t, storage.Ready(ctx, db), "readiness confirms the pipeline")
	assert.Equal(t, int64(4), scalarCount(t, db,
		"SELECT count() FROM system.tables WHERE database = 'reporting' AND name LIKE 'kafka_%'"))
	assert.Equal(t, int64(4), scalarCount(t, db,
		"SELECT count() FROM system.tables WHERE database = 'reporting' AND name LIKE '%_mv'"))

	paymentID := "pay-" + uuid.NewString()
	transactionID := "tx-" + uuid.NewString()
	payoutID := "po-" + uuid.NewString()
	alertID := uuid.NewString()
	payment, err := producer.PaymentCompleted(uuid.NewString(), paymentID, "m-1")
	require.NoError(t, err)
	ledger, err := producer.LedgerPosted(uuid.NewString(), transactionID, paymentID)
	require.NoError(t, err)
	payout, err := producer.PayoutRequested(uuid.NewString(), payoutID, "m-1")
	require.NoError(t, err)
	alert, err := producer.ReconAmountMismatch(alertID, payoutID, "m-1")
	require.NoError(t, err)
	require.NoError(t, producer.Publish(ctx, externalBrokers, []producer.Event{payment, ledger, payout, alert}))

	waitForCount(t, db, fmt.Sprintf(
		"SELECT count() FROM reporting.fact_payment_events FINAL WHERE payment_id = '%s'", paymentID), 1)
	waitForCount(t, db, fmt.Sprintf(
		"SELECT count() FROM reporting.fact_ledger_entries FINAL WHERE transaction_id = '%s'", transactionID), 2)
	waitForCount(t, db, fmt.Sprintf(
		"SELECT count() FROM reporting.fact_payouts FINAL WHERE payout_id = '%s'", payoutID), 1)
	waitForCount(t, db, fmt.Sprintf(
		"SELECT count() FROM reporting.fact_recon_alerts FINAL WHERE alert_id = '%s'", alertID), 1)

	var eventType, asset string
	var expectedAmount int64
	var decimals uint8
	var occurredAt time.Time
	require.NoError(t, db.QueryRowContext(ctx, fmt.Sprintf(`SELECT event_type, expected_amount, asset, decimals, occurred_at
FROM reporting.fact_payment_events FINAL WHERE payment_id = '%s'`, paymentID)).
		Scan(&eventType, &expectedAmount, &asset, &decimals, &occurredAt))
	assert.Equal(t, "payment.completed", eventType, "event type from the envelope header")
	assert.Equal(t, int64(100_000000), expectedAmount)
	assert.Equal(t, "USDT", asset)
	assert.Equal(t, uint8(6), decimals)
	assert.Equal(t, producer.OccurredAt.UnixMilli(), occurredAt.UnixMilli(),
		"millisecond time precision from the event body")

	var entryIndex uint8
	var direction string
	require.NoError(t, db.QueryRowContext(ctx, fmt.Sprintf(`SELECT entry_index, direction
FROM reporting.fact_ledger_entries FINAL WHERE transaction_id = '%s' AND account_id = 'acc-merchant'`, transactionID)).
		Scan(&entryIndex, &direction))
	assert.Equal(t, uint8(1), entryIndex, "entry_index 0-based")
	assert.Equal(t, "credit", direction)

	var refundInvoiceID string
	require.NoError(t, db.QueryRowContext(ctx, fmt.Sprintf(
		"SELECT refund_invoice_id FROM reporting.fact_payouts FINAL WHERE payout_id = '%s'", payoutID)).
		Scan(&refundInvoiceID))
	assert.Empty(t, refundInvoiceID)

	var alertType, severity string
	require.NoError(t, db.QueryRowContext(ctx, fmt.Sprintf(
		"SELECT alert_type, severity FROM reporting.fact_recon_alerts FINAL WHERE alert_id = '%s'", alertID)).
		Scan(&alertType, &severity))
	assert.Equal(t, "amount_mismatch", alertType)
	assert.Equal(t, "warning", severity)

	// Redelivery of the same event collapses via FINAL deduplication: the
	// duplicate and the sentinel go to the same partition (the same key);
	// waiting for the sentinel proves the duplicate was processed without
	// observing an intermediate state (the background ReplacingMergeTree
	// merge could collapse the rows earlier).
	duplicateSentinelID := "pay-" + uuid.NewString()
	duplicateSentinel, err := producer.PaymentCompleted(uuid.NewString(), duplicateSentinelID, "m-1")
	require.NoError(t, err)
	duplicateSentinel.Key = paymentID
	require.NoError(t, producer.Publish(ctx, externalBrokers, []producer.Event{payment, duplicateSentinel}))
	waitForCount(t, db, fmt.Sprintf(
		"SELECT count() FROM reporting.fact_payment_events FINAL WHERE payment_id = '%s'", duplicateSentinelID), 1)
	assert.Equal(t, int64(1), scalarCount(t, db, fmt.Sprintf(
		"SELECT count() FROM reporting.fact_payment_events FINAL WHERE payment_id = '%s'", paymentID)))

	// Invalid messages do not reach the facts, the pipeline stays alive:
	// decimals out of range, empty event_id, reserve.created without entries,
	// direction=0.
	badDecimalsID := "pay-" + uuid.NewString()
	badDecimals, err := producer.PaymentCompletedDecimals(uuid.NewString(), badDecimalsID, "m-1", 300)
	require.NoError(t, err)
	emptyEventID, err := producer.PaymentCompleted(uuid.NewString(), "pay-"+uuid.NewString(), "m-1")
	require.NoError(t, err)
	emptyEventID.Headers["event_id"] = ""
	reserveEventID := uuid.NewString()
	reserve, err := producer.ReserveCreated(reserveEventID, "po-"+uuid.NewString())
	require.NoError(t, err)
	unspecifiedTxID := "tx-" + uuid.NewString()
	unspecifiedEventID := uuid.NewString()
	unspecified, err := producer.LedgerUnspecifiedDirection(unspecifiedEventID, unspecifiedTxID, paymentID)
	require.NoError(t, err)
	sentinelID := "pay-" + uuid.NewString()
	sentinel, err := producer.PaymentCompleted(uuid.NewString(), sentinelID, "m-1")
	require.NoError(t, err)
	sentinel.Key = paymentID
	ledgerSentinelTxID := "tx-" + uuid.NewString()
	ledgerSentinel, err := producer.LedgerPosted(uuid.NewString(), ledgerSentinelTxID, paymentID)
	require.NoError(t, err)
	ledgerSentinel.Key = "acc-clearing"
	unspecified.Key = "acc-clearing"
	reserve.Key = ledgerSentinel.Key
	require.NoError(t, producer.Publish(ctx, externalBrokers,
		[]producer.Event{badDecimals, emptyEventID, sentinel}))
	require.NoError(t, producer.Publish(ctx, externalBrokers,
		[]producer.Event{reserve, unspecified, ledgerSentinel}))
	waitForCount(t, db, fmt.Sprintf(
		"SELECT count() FROM reporting.fact_payment_events FINAL WHERE payment_id = '%s'", sentinelID), 1)
	waitForCount(t, db, fmt.Sprintf(
		"SELECT count() FROM reporting.fact_ledger_entries FINAL WHERE transaction_id = '%s'", ledgerSentinelTxID), 2)
	assert.Equal(t, int64(0), scalarCount(t, db, fmt.Sprintf(
		"SELECT count() FROM reporting.fact_payment_events FINAL WHERE payment_id = '%s'", badDecimalsID)),
		"decimals out of range is filtered out")
	assert.Equal(t, int64(0), scalarCount(t, db,
		"SELECT count() FROM reporting.fact_payment_events FINAL WHERE event_id = ''"),
		"an empty event_id is filtered out")
	assert.Equal(t, int64(0), scalarCount(t, db, fmt.Sprintf(
		"SELECT count() FROM reporting.fact_ledger_entries FINAL WHERE event_id = '%s'", reserveEventID)),
		"reserve.created without entries creates no rows")
	assert.Equal(t, int64(0), scalarCount(t, db, fmt.Sprintf(
		"SELECT count() FROM reporting.fact_ledger_entries FINAL WHERE transaction_id = '%s'", unspecifiedTxID)),
		"direction=0 is filtered out")

	// A broken message does not stop the partition: the next valid one arrives.
	broken := producer.Event{Topic: "payment.events", Key: "broken", Headers: map[string]string{
		"event_id":   uuid.NewString(),
		"event_type": "payment.broken",
	}, Payload: []byte{0xff, 0xfe, 0xfd}}
	require.NoError(t, producer.Publish(ctx, externalBrokers, []producer.Event{broken}))
	afterBrokenID := "pay-" + uuid.NewString()
	afterBroken, err := producer.PaymentCompleted(uuid.NewString(), afterBrokenID, "m-1")
	require.NoError(t, err)
	require.NoError(t, producer.Publish(ctx, externalBrokers, []producer.Event{afterBroken}))
	waitForCount(t, db, fmt.Sprintf(
		"SELECT count() FROM reporting.fact_payment_events FINAL WHERE payment_id = '%s'", afterBrokenID), 1)

	// A configuration change (shrinking the broker list - a substring of the
	// old value) must be detected by the fingerprint and recreate the pair.
	changed, err := storage.NewPipeline(db, "kafka:2909", "events.proto", nil)
	require.NoError(t, err)
	require.NoError(t, changed.Reconcile(ctx))
	recreated := scalarCount(t, db, `SELECT count() FROM system.tables WHERE database = 'reporting'
AND name = 'kafka_payment_events' AND position(create_table_query, '''kafka:2909''') > 0
AND position(create_table_query, 'kafka:29092') = 0`)
	assert.Equal(t, int64(1), recreated, "shrinking the broker list recreates the table")

	// A partial state (the table exists, the view is dropped) is healed at
	// reconciliation with the same config, and before the healing it is
	// reflected in service readiness.
	_, err = db.ExecContext(ctx, "DROP VIEW reporting.fact_payment_events_mv")
	require.NoError(t, err)
	require.Error(t, storage.Ready(ctx, db), "readiness sees an incomplete pipeline")
	require.NoError(t, changed.Reconcile(ctx), "the config is current: view recovery branch")
	assert.Equal(t, int64(1), scalarCount(t, db, `SELECT count() FROM system.tables
WHERE database = 'reporting' AND name = 'fact_payment_events_mv'`), "the view is restored without recreating the table")

	// Restoring the original brokers recreates the pair again; fact data is
	// intact, the stream continues.
	restored, err := storage.NewPipeline(db, internalBrokers[0], "events.proto", nil)
	require.NoError(t, err)
	require.NoError(t, restored.Reconcile(ctx))
	assert.Equal(t, int64(1), scalarCount(t, db, `SELECT count() FROM system.tables
WHERE database = 'reporting' AND name = 'kafka_payment_events'
AND position(create_table_query, '''`+internalBrokers[0]+`''') > 0`), "the original brokers are applied")

	// Fact data is not affected by pipeline recreation, the stream continues.
	assert.Equal(t, int64(1), scalarCount(t, db, fmt.Sprintf(
		"SELECT count() FROM reporting.fact_payment_events FINAL WHERE payment_id = '%s'", paymentID)))
	afterRestoreID := "pay-" + uuid.NewString()
	afterRestore, err := producer.PaymentCompleted(uuid.NewString(), afterRestoreID, "m-1")
	require.NoError(t, err)
	require.NoError(t, producer.Publish(ctx, externalBrokers, []producer.Event{afterRestore}))
	waitForCount(t, db, fmt.Sprintf(
		"SELECT count() FROM reporting.fact_payment_events FINAL WHERE payment_id = '%s'", afterRestoreID), 1)
}

// TestLedgerEnrichmentIntegration checks the enrichment of ledger rows with
// the dim_accounts dictionary on ingestion (MV JOIN): a dictionary account
// gets account_type and merchant_id, an account outside the dictionary stays
// empty until backfill (CBL-11.3, FR-3).
func TestLedgerEnrichmentIntegration(t *testing.T) {
	ctx := context.Background()
	network := startNetwork(t)
	internalBrokers, externalBrokers := startKafka(t, network)
	db := startClickHouse(t, network)
	require.NoError(t, storage.Migrate(ctx, db, filepath.Join("..", "..", "migrations")))

	// The dictionary is populated before the pipeline start: a row for
	// acc-merchant (acc-clearing is not registered in the chart and stays
	// without values).
	require.NoError(t, storage.InsertDimAccounts(ctx, db, []storage.DimAccount{{
		AccountID: "acc-merchant", AccountType: "merchant", MerchantID: "m-1", Asset: "USDT", Decimals: 6,
	}}))

	pipeline, err := storage.NewPipeline(db, internalBrokers[0], "events.proto", nil)
	require.NoError(t, err)
	require.NoError(t, pipeline.Reconcile(ctx))

	transactionID := "tx-" + uuid.NewString()
	ledger, err := producer.LedgerPosted(uuid.NewString(), transactionID, "pay-"+uuid.NewString())
	require.NoError(t, err)
	require.NoError(t, producer.Publish(ctx, externalBrokers, []producer.Event{ledger}))
	waitForCount(t, db, fmt.Sprintf(
		"SELECT count() FROM reporting.fact_ledger_entries FINAL WHERE transaction_id = '%s'", transactionID), 2)

	var accountType, merchantID string
	require.NoError(t, db.QueryRowContext(ctx, fmt.Sprintf(`SELECT account_type, merchant_id
FROM reporting.fact_ledger_entries FINAL WHERE transaction_id = '%s' AND account_id = 'acc-merchant'`, transactionID)).
		Scan(&accountType, &merchantID))
	assert.Equal(t, "merchant", accountType, "a dictionary account enriches the row on ingestion")
	assert.Equal(t, "m-1", merchantID)

	require.NoError(t, db.QueryRowContext(ctx, fmt.Sprintf(`SELECT account_type, merchant_id
FROM reporting.fact_ledger_entries FINAL WHERE transaction_id = '%s' AND account_id = 'acc-clearing'`, transactionID)).
		Scan(&accountType, &merchantID))
	assert.Empty(t, accountType, "an account outside the dictionary stays empty")
	assert.Empty(t, merchantID)

	// A dictionary record appearing retroactively is healed by a backfill
	// mutation (checked by the vocabulary cycle in the vocabulary
	// integration tests).
	require.NoError(t, storage.BackfillAccount(ctx, db, storage.DimAccount{
		AccountID: "acc-clearing", AccountType: "clearing", MerchantID: "", Asset: "USDT", Decimals: 6,
	}))
	enriched := 0
	deadline := time.Now().Add(30 * time.Second)
	for {
		require.NoError(t, db.QueryRowContext(ctx, fmt.Sprintf(`SELECT count()
FROM reporting.fact_ledger_entries FINAL WHERE transaction_id = '%s' AND account_id = 'acc-clearing' AND account_type = 'clearing'`, transactionID)).
			Scan(&enriched))
		if enriched == 1 || time.Now().After(deadline) {
			break
		}
		time.Sleep(time.Second)
	}
	assert.Equal(t, 1, enriched, "the backfill mutation enriched the historical row")
}
