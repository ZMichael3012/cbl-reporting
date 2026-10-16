package storage_test

import (
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cbl/cbl-reporting/internal/storage"
)

// newTestDB opens a connection without connecting (the dsn is not resolved until a query).
func newTestDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := storage.Open("clickhouse://cbl:cbl@127.0.0.1:1/reporting")
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// TestOpenInvalidDSN checks rejection of a broken DSN and sanitization of the
// message (the password does not reach the error and logs).
func TestOpenInvalidDSN(t *testing.T) {
	_, err := storage.Open("://missing-scheme")
	require.Error(t, err)
	require.NotContains(t, err.Error(), "://missing-scheme")

	_, err = storage.Open("clickhouse://cbl:supersecret@host:notaport/reporting")
	require.Error(t, err)
	require.NotContains(t, err.Error(), "supersecret", "the password is not echoed")
}

// TestReadyUnavailable checks readiness failure when the storage is unavailable.
func TestReadyUnavailable(t *testing.T) {
	db := newTestDB(t)
	require.NoError(t, db.Close())
	require.Error(t, storage.Ready(t.Context(), db))
}

// TestPingClosedDB checks the availability error (readyz on failure).
func TestPingClosedDB(t *testing.T) {
	db := newTestDB(t)
	require.NoError(t, db.Close())
	require.Error(t, storage.Ping(t.Context(), db))
}

// TestMigrateInvalidDir checks the migration error wrapper.
func TestMigrateInvalidDir(t *testing.T) {
	db := newTestDB(t)
	err := storage.Migrate(t.Context(), db, "missing-migrations-dir")
	require.Error(t, err)
	require.Contains(t, err.Error(), "goose")
}

// TestReconcileUnavailableDB checks the state read error wrapper.
func TestReconcileUnavailableDB(t *testing.T) {
	db := newTestDB(t)
	require.NoError(t, db.Close())
	pipeline, err := storage.NewPipeline(db, "kafka:9092", "events.proto", nil)
	require.NoError(t, err)
	err = pipeline.Reconcile(t.Context())
	require.Error(t, err)
	require.Contains(t, err.Error(), "read table state")
}

// TestNewPipelineValidation checks the fail-fast configuration validation
// (values are interpolated into the DDL, injection via the environment is
// excluded).
func TestNewPipelineValidation(t *testing.T) {
	db := newTestDB(t)
	_, err := storage.NewPipeline(db, "kafka:9092; DROP TABLE x", "events.proto", nil)
	require.Error(t, err, "unexpected brokers are rejected")
	_, err = storage.NewPipeline(db, "kafka:9092,", "events.proto", nil)
	require.Error(t, err, "a trailing comma in brokers is rejected")
	_, err = storage.NewPipeline(db, "kafka:9092", "'; DROP TABLE x --", nil)
	require.Error(t, err, "an unexpected schema path is rejected")
	_, err = storage.NewPipeline(db, "kafka:9092", "../secrets.proto", nil)
	require.Error(t, err, "upward traversal in the schema path is rejected")
	_, err = storage.NewPipeline(db, "kafka:9092", "", nil)
	require.Error(t, err, "an empty schema path is rejected")
	_, err = storage.NewPipeline(nil, "kafka:9092", "events.proto", nil)
	require.Error(t, err, "the pipeline cannot be assembled without a connection")
	pipeline, err := storage.NewPipeline(db, "kafka:9092,kafka2:9092", "events.proto", nil)
	require.NoError(t, err)
	require.NotNil(t, pipeline)
}

// TestBuildKafkaTableDDLComment checks the configuration fingerprint in
// COMMENT: a change of brokers or schema changes the fingerprint (pair
// recreation).
func TestBuildKafkaTableDDLComment(t *testing.T) {
	spec := storage.DefaultSpecs()[0]
	first := storage.BuildKafkaTableDDL(spec, "kafka:9092", "events.proto", "cbl-pipeline:aaaa")
	assert.Contains(t, first, "COMMENT 'cbl-pipeline:aaaa'")
	second := storage.BuildKafkaTableDDL(spec, "kafka:2909", "events.proto", "cbl-pipeline:bbbb")
	assert.NotEqual(t, first, second, "shrinking the broker list changes the DDL")
	assert.NotContains(t, storage.BuildKafkaTableDDL(spec, "kafka:9092", "events.proto", ""), "COMMENT")
}

// TestDefaultSpecs checks the composition of the pipelines: four topics and table pairs.
func TestDefaultSpecs(t *testing.T) {
	specs := storage.DefaultSpecs()
	require.Len(t, specs, 4)
	topics := map[string]bool{}
	for _, spec := range specs {
		topics[spec.Topic] = true
		assert.NotEmpty(t, spec.FactTable, spec.Topic)
		assert.NotEmpty(t, spec.KafkaTable, spec.Topic)
		assert.NotEmpty(t, spec.ViewName, spec.Topic)
		assert.NotEmpty(t, spec.Message, spec.Topic)
	}
	assert.Equal(t, map[string]bool{
		"payment.events": true, "ledger.events": true,
		"payout.events": true, "recon.alerts": true,
	}, topics)
}

// TestBuildKafkaTableDDL checks the composition of the Kafka table DDL.
func TestBuildKafkaTableDDL(t *testing.T) {
	spec := storage.DefaultSpecs()[0]
	ddl := storage.BuildKafkaTableDDL(spec, "kafka:9092", "events.proto", "")
	for _, fragment := range []string{
		"CREATE TABLE IF NOT EXISTS reporting.kafka_payment_events",
		"ENGINE = Kafka",
		"kafka_broker_list = 'kafka:9092'",
		"kafka_topic_list = 'payment.events'",
		"kafka_group_name = 'cbl-reporting-payment'",
		"kafka_format = 'ProtobufSingle'",
		"format_schema = 'events.proto:PaymentRow'",
		"kafka_handle_error_mode = 'stream'",
		"kafka_skip_broken_messages = 1000",
		"Tuple(minor_units Int64, asset String, decimals UInt32)",
		"occurred_at Tuple(seconds Int64, nanos Int32)",
	} {
		assert.Contains(t, ddl, fragment)
	}
}

// TestBuildViewDDL checks the filters and the materialized view mapping.
func TestBuildViewDDL(t *testing.T) {
	var ledgerView, alertsView, paymentView, payoutView string
	for _, spec := range storage.DefaultSpecs() {
		switch spec.Topic {
		case "ledger.events":
			ledgerView = storage.BuildViewDDL(spec)
		case "recon.alerts":
			alertsView = storage.BuildViewDDL(spec)
		case "payment.events":
			paymentView = storage.BuildViewDDL(spec)
		case "payout.events":
			payoutView = storage.BuildViewDDL(spec)
		}
	}
	for _, fragment := range []string{
		"ARRAY JOIN entries AS entry, arrayEnumerate(entries) AS ordinality",
		"toUInt8(ordinality - 1) AS entry_index",
		"entry.2 IN (1, 2)",
		"length(entries) <= 255",
		"transaction_id != ''",
		"tupleElement(entry.3, 3) <= 18",
		"CAST(if(entry.2 = 1, 'debit', 'credit') AS Enum8('debit' = 1, 'credit' = 2))",
		// The dictionary enriches new rows on ingestion; FINAL prevents row
		// multiplication on unmerged dictionary duplicates (CBL-11.3).
		"LEFT JOIN (SELECT account_id, account_type, merchant_id FROM reporting.dim_accounts FINAL) AS dim",
		"dim.account_type AS account_type, dim.merchant_id AS merchant_id",
		"ON flat.account_id = dim.account_id",
	} {
		assert.Contains(t, ledgerView, fragment)
	}
	for _, fragment := range []string{
		"multiIf(type_code = 1, 'missing_in_ledger'",
		"severity_code = 3, 'critical'",
		"FROM (SELECT alert_id, type AS type_code, ref_id, severity AS severity_code,",
		"type_code IN (1, 2, 3) AND severity_code IN (1, 2, 3)",
	} {
		assert.Contains(t, alertsView, fragment)
	}
	// The subquery renames the tuples: an alias matching a column name
	// shadows it for the remaining SELECT expressions (verified on ClickHouse).
	for _, fragment := range []string{
		"(SELECT payment_id, merchant_id, quote_id, rate, txid, state_version, _headers,",
		"expected_amount AS amount_tuple",
		"amount_tuple.1 AS expected_amount",
		"toDateTime64(occurred_tuple.1, 3, 'UTC')",
		"+ toIntervalNanosecond(toInt64(occurred_tuple.2)) AS occurred_at",
		"tupleElement(amount_tuple, 3) <= 18",
		"arrayFirst(x -> x.1 = 'event_id', _headers).2 != '' AND payment_id != ''",
	} {
		assert.Contains(t, paymentView, fragment)
	}
	for _, fragment := range []string{
		"amount AS amount_tuple, fee AS fee_tuple, occurred_at AS occurred_tuple",
		"amount_tuple.1 AS amount, fee_tuple.1 AS fee_amount",
		"tupleElement(amount_tuple, 3) <= 18",
	} {
		assert.Contains(t, payoutView, fragment)
	}
}
