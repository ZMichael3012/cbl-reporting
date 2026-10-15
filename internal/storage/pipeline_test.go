package storage_test

import (
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cbl/cbl-reporting/internal/storage"
)

// newTestDB открывает подключение без соединения (dsn не резолвится до запроса).
func newTestDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := storage.Open("clickhouse://cbl:cbl@127.0.0.1:1/reporting")
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// TestOpenInvalidDSN проверяет отказ на битом DSN и санитизацию сообщения
// (пароль не попадает в ошибку и логи).
func TestOpenInvalidDSN(t *testing.T) {
	_, err := storage.Open("://missing-scheme")
	require.Error(t, err)
	require.NotContains(t, err.Error(), "://missing-scheme")

	_, err = storage.Open("clickhouse://cbl:supersecret@host:notaport/reporting")
	require.Error(t, err)
	require.NotContains(t, err.Error(), "supersecret", "пароль не эхоится")
}

// TestReadyUnavailable проверяет отказ готовности при недоступном хранилище.
func TestReadyUnavailable(t *testing.T) {
	db := newTestDB(t)
	require.NoError(t, db.Close())
	require.Error(t, storage.Ready(t.Context(), db))
}

// TestPingClosedDB проверяет ошибку доступности (readyz при сбое).
func TestPingClosedDB(t *testing.T) {
	db := newTestDB(t)
	require.NoError(t, db.Close())
	require.Error(t, storage.Ping(t.Context(), db))
}

// TestMigrateInvalidDir проверяет обертку ошибки миграций.
func TestMigrateInvalidDir(t *testing.T) {
	db := newTestDB(t)
	err := storage.Migrate(t.Context(), db, "missing-migrations-dir")
	require.Error(t, err)
	require.Contains(t, err.Error(), "goose")
}

// TestReconcileUnavailableDB проверяет обертку ошибки чтения состояния.
func TestReconcileUnavailableDB(t *testing.T) {
	db := newTestDB(t)
	require.NoError(t, db.Close())
	pipeline, err := storage.NewPipeline(db, "kafka:9092", "events.proto", nil)
	require.NoError(t, err)
	err = pipeline.Reconcile(t.Context())
	require.Error(t, err)
	require.Contains(t, err.Error(), "read table state")
}

// TestNewPipelineValidation проверяет fail-fast проверку конфигурации
// (значения подставляются в DDL, инъекция через окружение исключена).
func TestNewPipelineValidation(t *testing.T) {
	db := newTestDB(t)
	_, err := storage.NewPipeline(db, "kafka:9092; DROP TABLE x", "events.proto", nil)
	require.Error(t, err, "недопустимые брокеры отклоняются")
	_, err = storage.NewPipeline(db, "kafka:9092,", "events.proto", nil)
	require.Error(t, err, "висячая запятая в брокерах отклоняется")
	_, err = storage.NewPipeline(db, "kafka:9092", "'; DROP TABLE x --", nil)
	require.Error(t, err, "недопустимый путь схемы отклоняется")
	_, err = storage.NewPipeline(db, "kafka:9092", "../secrets.proto", nil)
	require.Error(t, err, "переход вверх по пути схемы отклоняется")
	_, err = storage.NewPipeline(db, "kafka:9092", "", nil)
	require.Error(t, err, "пустой путь схемы отклоняется")
	_, err = storage.NewPipeline(nil, "kafka:9092", "events.proto", nil)
	require.Error(t, err, "без подключения сборка невозможна")
	pipeline, err := storage.NewPipeline(db, "kafka:9092,kafka2:9092", "events.proto", nil)
	require.NoError(t, err)
	require.NotNil(t, pipeline)
}

// TestBuildKafkaTableDDLComment проверяет отпечаток конфигурации в COMMENT:
// смена брокеров или схемы меняет отпечаток (пересоздание пары).
func TestBuildKafkaTableDDLComment(t *testing.T) {
	spec := storage.DefaultSpecs()[0]
	first := storage.BuildKafkaTableDDL(spec, "kafka:9092", "events.proto", "cbl-pipeline:aaaa")
	assert.Contains(t, first, "COMMENT 'cbl-pipeline:aaaa'")
	second := storage.BuildKafkaTableDDL(spec, "kafka:2909", "events.proto", "cbl-pipeline:bbbb")
	assert.NotEqual(t, first, second, "сокращение списка брокеров меняет DDL")
	assert.NotContains(t, storage.BuildKafkaTableDDL(spec, "kafka:9092", "events.proto", ""), "COMMENT")
}

// TestDefaultSpecs проверяет состав конвейеров: четыре топика и пары таблиц.
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

// TestBuildKafkaTableDDL проверяет состав DDL Kafka-таблицы.
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

// TestBuildViewDDL проверяет фильтры и маппинг materialized view.
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
	// Подзапрос переименовывает кортежи: алиас, совпадающий с именем колонки,
	// перекрывает ее для остальных выражений SELECT (проверено на ClickHouse).
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
