package storage

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"regexp"
	"strings"

	"go.uber.org/zap"
)

// hostPortPattern defines the allowed form of a single broker in the list
// (protection of SQL from injection via the environment configuration).
var hostPortPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9.-]*:[0-9]{1,5}$`)

// schemaPathPattern defines the allowed form of the wire schema path: a file
// name relative to the ClickHouse server format_schema_path (the Kafka engine
// forbids an absolute path; upward traversal is forbidden separately).
var schemaPathPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9/._-]*$`)

// TopicSpec describes a fact pipeline: the Kafka table and the materialized view.
type TopicSpec struct {
	// FactTable defines the target fact table from migrations.
	FactTable string
	// KafkaTable defines the Kafka engine table.
	KafkaTable string
	// ViewName defines the materialized view name.
	ViewName string
	// Topic defines the Kafka topic.
	Topic string
	// Group defines the ClickHouse consumer group.
	Group string
	// Message defines the wire schema message (deploy/format_schemas/events.proto).
	Message string
	// Columns defines the DDL of the Kafka table columns.
	Columns string
	// Select defines the materialized view query (without FROM).
	Select string
}

// Pipeline reconciles Kafka tables and materialized views of ClickHouse.
type Pipeline struct {
	db         *sql.DB
	brokers    string
	schemaPath string
	specs      []TopicSpec
	log        *zap.Logger
}

// NewPipeline assembles the pipeline; brokers and the schema path are
// validated fail-fast (the values are interpolated into the reconciliation
// DDL).
func NewPipeline(db *sql.DB, brokers, schemaPath string, log *zap.Logger) (*Pipeline, error) {
	if db == nil {
		return nil, errors.New("clickhouse db is required")
	}
	for _, broker := range strings.Split(brokers, ",") {
		if !hostPortPattern.MatchString(broker) {
			return nil, fmt.Errorf("KAFKA_BROKERS has unexpected form: %q", brokers)
		}
	}
	if !schemaPathPattern.MatchString(schemaPath) || strings.Contains(schemaPath, "..") {
		return nil, fmt.Errorf("SCHEMA_PATH has unexpected form: %q", schemaPath)
	}
	if log == nil {
		log = zap.NewNop()
	}
	return &Pipeline{db: db, brokers: brokers, schemaPath: schemaPath, specs: DefaultSpecs(), log: log}, nil
}

// Reconcile brings Kafka tables and materialized views to the environment
// configuration and the definitions of the current binary: it creates missing
// pairs, recreates a pair on drift (brokers, schema, columns, query) and
// heals a partial state (a table without a view). Fact data is not affected.
func (p *Pipeline) Reconcile(ctx context.Context) error {
	for _, spec := range p.specs {
		if err := p.reconcileTopic(ctx, spec); err != nil {
			return fmt.Errorf("%s: %w", spec.KafkaTable, err)
		}
	}
	return nil
}

// reconcileTopic compares the actual state of a table+view pair with the
// expected DDL fingerprint and brings it to the expected form.
func (p *Pipeline) reconcileTopic(ctx context.Context, spec TopicSpec) error {
	expectedComment := p.fingerprint(spec)
	var actualComment string
	err := p.db.QueryRowContext(ctx,
		"SELECT comment FROM system.tables WHERE database = 'reporting' AND name = ?",
		spec.KafkaTable).Scan(&actualComment)
	tableMissing := errors.Is(err, sql.ErrNoRows)
	if err != nil && !tableMissing {
		return fmt.Errorf("read table state: %w", err)
	}

	switch {
	case tableMissing:
		// the pair is missing: create it
	case actualComment != expectedComment:
		// definitions (SQL or environment configuration) changed:
		// recreate the whole pair, otherwise the MV would stay on the old definition
		p.log.Warn("kafka pipeline configuration changed, recreating",
			zap.String("table", spec.KafkaTable),
			zap.String("brokers", p.brokers))
		if _, dropErr := p.db.ExecContext(ctx, "DROP VIEW IF EXISTS reporting."+spec.ViewName); dropErr != nil {
			return fmt.Errorf("drop view: %w", dropErr)
		}
		if _, dropErr := p.db.ExecContext(ctx, "DROP TABLE IF EXISTS reporting."+spec.KafkaTable); dropErr != nil {
			return fmt.Errorf("drop kafka table: %w", dropErr)
		}
		tableMissing = true
	default:
		// the table is up to date: the view could be missing (a failure between CREATEs)
		var viewExists uint64
		if err := p.db.QueryRowContext(ctx,
			"SELECT count() FROM system.tables WHERE database = 'reporting' AND name = ?",
			spec.ViewName).Scan(&viewExists); err != nil {
			return fmt.Errorf("read view state: %w", err)
		}
		if viewExists == 0 {
			p.log.Warn("materialized view is missing, recreating", zap.String("view", spec.ViewName))
			if _, err := p.db.ExecContext(ctx, BuildViewDDL(spec)); err != nil {
				return fmt.Errorf("create materialized view: %w", err)
			}
		}
		return nil
	}
	if tableMissing {
		if _, err := p.db.ExecContext(ctx, BuildKafkaTableDDL(spec, p.brokers, p.schemaPath, expectedComment)); err != nil {
			return fmt.Errorf("create kafka table: %w", err)
		}
		if _, err := p.db.ExecContext(ctx, BuildViewDDL(spec)); err != nil {
			return fmt.Errorf("create materialized view: %w", err)
		}
	}
	return nil
}

// fingerprint returns the fingerprint of the expected pair definition (SQL of
// both objects): a change of brokers, schema, columns or query changes the
// fingerprint and triggers recreation.
func (p *Pipeline) fingerprint(spec TopicSpec) string {
	sum := sha256.Sum256([]byte(
		BuildKafkaTableDDL(spec, p.brokers, p.schemaPath, "") + "\n" + BuildViewDDL(spec)))
	return "cbl-pipeline:" + hex.EncodeToString(sum[:])[:16]
}

// BuildKafkaTableDDL builds the Kafka engine table DDL for the topic; comment
// defines the configuration fingerprint (an empty string - without COMMENT).
func BuildKafkaTableDDL(spec TopicSpec, brokers, schemaPath, comment string) string {
	ddl := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS reporting.%s (%s)
ENGINE = Kafka
SETTINGS kafka_broker_list = '%s', kafka_topic_list = '%s', kafka_group_name = '%s',
kafka_format = 'ProtobufSingle', format_schema = '%s:%s',
kafka_handle_error_mode = 'stream', kafka_skip_broken_messages = 1000,
kafka_num_consumers = 1`,
		spec.KafkaTable, spec.Columns, brokers, spec.Topic, spec.Group, schemaPath, spec.Message)
	if comment != "" {
		ddl += fmt.Sprintf("\nCOMMENT '%s'", comment)
	}
	return ddl
}

// BuildViewDDL builds the materialized view DDL that writes to the fact table.
func BuildViewDDL(spec TopicSpec) string {
	return fmt.Sprintf("CREATE MATERIALIZED VIEW IF NOT EXISTS reporting.%s TO reporting.%s AS %s",
		spec.ViewName, spec.FactTable, spec.Select)
}

// DefaultSpecs describes the pipelines of the four topics (CBL-003 section 6,
// column mapping - CBL-11.1 section 6).
func DefaultSpecs() []TopicSpec {
	return []TopicSpec{
		{
			FactTable:  "fact_payment_events",
			KafkaTable: "kafka_payment_events",
			ViewName:   "fact_payment_events_mv",
			Topic:      "payment.events",
			Group:      "cbl-reporting-payment",
			Message:    "PaymentRow",
			Columns: `payment_id String, merchant_id String,
expected_amount Tuple(minor_units Int64, asset String, decimals UInt32),
received_amount Tuple(minor_units Int64, asset String, decimals UInt32),
fee Tuple(minor_units Int64, asset String, decimals UInt32),
quote_id String, rate String, txid String, state_version UInt64,
occurred_at Tuple(seconds Int64, nanos Int32)`,
			// The subquery renames the tuples: an alias matching a column
			// name shadows it for the remaining SELECT expressions (verified
			// on ClickHouse 24.8). Time is seconds plus nanoseconds; the
			// resulting column rounds to milliseconds (DateTime64(3)).
			Select: `SELECT arrayFirst(x -> x.1 = 'event_id', _headers).2 AS event_id, payment_id, merchant_id,
arrayFirst(x -> x.1 = 'event_type', _headers).2 AS event_type,
amount_tuple.2 AS asset, toUInt8(amount_tuple.3) AS decimals,
amount_tuple.1 AS expected_amount, received_tuple.1 AS received_amount,
fee_tuple.1 AS fee_amount, quote_id, rate, txid, state_version,
toDateTime64(occurred_tuple.1, 3, 'UTC')
    + toIntervalNanosecond(toInt64(occurred_tuple.2)) AS occurred_at
FROM (SELECT payment_id, merchant_id, quote_id, rate, txid, state_version, _headers,
             expected_amount AS amount_tuple, received_amount AS received_tuple,
             fee AS fee_tuple, occurred_at AS occurred_tuple
      FROM reporting.kafka_payment_events)
WHERE arrayFirst(x -> x.1 = 'event_id', _headers).2 != '' AND payment_id != ''
AND tupleElement(amount_tuple, 3) <= 18`,
		},
		{
			FactTable:  "fact_ledger_entries",
			KafkaTable: "kafka_ledger_events",
			ViewName:   "fact_ledger_entries_mv",
			Topic:      "ledger.events",
			Group:      "cbl-reporting-ledger",
			Message:    "LedgerRow",
			Columns: `transaction_id String, ref_type String, ref_id String,
entries Array(Tuple(account_id String, direction Int32,
amount Tuple(minor_units Int64, asset String, decimals UInt32))),
occurred_at Tuple(seconds Int64, nanos Int32)`,
			// The subquery renames the tuples (the alias shadows the
			// same-named column, verified on 24.8) and hoists the ARRAY JOIN;
			// the dim_accounts dictionary enriches new rows on ingestion
			// (CBL-11.3, FR-3): FINAL on the right side prevents unmerged
			// duplicates from multiplying rows; a missing account yields
			// empty values (backfill catches up).
			Select: `SELECT flat.event_id, flat.transaction_id, flat.entry_index, flat.account_id,
dim.account_type AS account_type, dim.merchant_id AS merchant_id,
flat.asset, flat.decimals, flat.direction, flat.amount, flat.ref_type, flat.ref_id,
flat.event_type, flat.occurred_at
FROM (
SELECT arrayFirst(x -> x.1 = 'event_id', _headers).2 AS event_id, transaction_id,
toUInt8(ordinality - 1) AS entry_index, entry.1 AS account_id,
tupleElement(entry.3, 2) AS asset, toUInt8(tupleElement(entry.3, 3)) AS decimals,
CAST(if(entry.2 = 1, 'debit', 'credit') AS Enum8('debit' = 1, 'credit' = 2)) AS direction,
tupleElement(entry.3, 1) AS amount, ref_type, ref_id,
arrayFirst(x -> x.1 = 'event_type', _headers).2 AS event_type,
toDateTime64(occurred_at.1, 3, 'UTC')
    + toIntervalNanosecond(toInt64(occurred_at.2)) AS occurred_at
FROM reporting.kafka_ledger_events
ARRAY JOIN entries AS entry, arrayEnumerate(entries) AS ordinality
WHERE arrayFirst(x -> x.1 = 'event_id', _headers).2 != '' AND transaction_id != ''
AND entry.2 IN (1, 2) AND length(entries) <= 255
AND tupleElement(entry.3, 3) <= 18
) AS flat
LEFT JOIN (SELECT account_id, account_type, merchant_id FROM reporting.dim_accounts FINAL) AS dim
ON flat.account_id = dim.account_id`,
		},
		{
			FactTable:  "fact_payouts",
			KafkaTable: "kafka_payout_events",
			ViewName:   "fact_payouts_mv",
			Topic:      "payout.events",
			Group:      "cbl-reporting-payout",
			Message:    "PayoutRow",
			Columns: `payout_id String, merchant_id String,
amount Tuple(minor_units Int64, asset String, decimals UInt32),
fee Tuple(minor_units Int64, asset String, decimals UInt32),
txid String, provider String, failure_reason String, refund_invoice_id String,
state_version UInt64, occurred_at Tuple(seconds Int64, nanos Int32)`,
			Select: `SELECT arrayFirst(x -> x.1 = 'event_id', _headers).2 AS event_id, payout_id, merchant_id,
arrayFirst(x -> x.1 = 'event_type', _headers).2 AS event_type,
amount_tuple.2 AS asset, toUInt8(amount_tuple.3) AS decimals,
amount_tuple.1 AS amount, fee_tuple.1 AS fee_amount,
txid, provider, failure_reason, refund_invoice_id, state_version,
toDateTime64(occurred_tuple.1, 3, 'UTC')
    + toIntervalNanosecond(toInt64(occurred_tuple.2)) AS occurred_at
FROM (SELECT payout_id, merchant_id, txid, provider, failure_reason,
             refund_invoice_id, state_version, _headers,
             amount AS amount_tuple, fee AS fee_tuple, occurred_at AS occurred_tuple
      FROM reporting.kafka_payout_events)
WHERE arrayFirst(x -> x.1 = 'event_id', _headers).2 != '' AND payout_id != ''
AND tupleElement(amount_tuple, 3) <= 18`,
		},
		{
			FactTable:  "fact_recon_alerts",
			KafkaTable: "kafka_recon_alerts",
			ViewName:   "fact_recon_alerts_mv",
			Topic:      "recon.alerts",
			Group:      "cbl-reporting-recon",
			Message:    "ReconAlertRow",
			Columns: `alert_id String, type Int32, ref_id String, severity Int32,
expected String, actual String, merchant_id String,
created_at Tuple(seconds Int64, nanos Int32)`,
			// The subquery renames type/severity: the severity alias would
			// shadow the same-named column in WHERE (verified on 24.8).
			Select: `SELECT alert_id,
multiIf(type_code = 1, 'missing_in_ledger', type_code = 2, 'amount_mismatch',
        type_code = 3, 'status_mismatch', '') AS alert_type,
ref_id,
multiIf(severity_code = 1, 'info', severity_code = 2, 'warning',
        severity_code = 3, 'critical', '') AS severity,
expected, actual, merchant_id,
toDateTime64(created_at.1, 3, 'UTC')
    + toIntervalNanosecond(toInt64(created_at.2)) AS created_at
FROM (SELECT alert_id, type AS type_code, ref_id, severity AS severity_code,
             expected, actual, merchant_id, created_at
      FROM reporting.kafka_recon_alerts)
WHERE alert_id != '' AND type_code IN (1, 2, 3) AND severity_code IN (1, 2, 3)`,
		},
	}
}
