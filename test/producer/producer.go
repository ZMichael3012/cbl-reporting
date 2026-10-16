// Package producer publishes test CBL events to Kafka: it is used by
// integration tests and the reporting service e2e (production code has no
// own Kafka client - consumption goes through the ClickHouse Kafka engine).
package producer

import (
	"context"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/cbl/cbl-protos/gen/go/billing/common/v1"
	eventsv1 "github.com/cbl/cbl-protos/gen/go/billing/events/v1"
)

// Event describes a CBL envelope message to publish.
type Event struct {
	// Topic sets the Kafka topic.
	Topic string
	// Key sets the partition key.
	Key string
	// Headers sets the envelope headers (event_id, event_type, occurred_at).
	Headers map[string]string
	// Payload sets the serialized protobuf event body.
	Payload []byte
}

// OccurredAt sets the common time of test events (fixed for assertions;
// non-zero nanoseconds verify the millisecond conversion precision).
var OccurredAt = time.Date(2026, 10, 15, 10, 0, 0, 123456789, time.UTC)

// NewEvent builds an event envelope with the mandatory headers.
func NewEvent(topic, eventType, eventID, key string, occurredAt time.Time, payload []byte) Event {
	return Event{
		Topic: topic,
		Key:   key,
		Headers: map[string]string{
			"event_id":    eventID,
			"event_type":  eventType,
			"occurred_at": occurredAt.UTC().Format(time.RFC3339Nano),
		},
		Payload: payload,
	}
}

// PaymentCompleted builds a payment.completed event.
func PaymentCompleted(eventID, paymentID, merchantID string) (Event, error) {
	return PaymentCompletedDecimals(eventID, paymentID, merchantID, 6)
}

// PaymentCompletedDecimals builds a payment.completed event with the given
// decimals (checks of the materialized view invalid value filter).
func PaymentCompletedDecimals(eventID, paymentID, merchantID string, decimals uint32) (Event, error) {
	event := &eventsv1.PaymentEvent{
		PaymentId:      paymentID,
		MerchantId:     merchantID,
		Type:           eventsv1.PaymentEventType_PAYMENT_EVENT_TYPE_COMPLETED,
		ExpectedAmount: &commonv1.Money{MinorUnits: 100_000000, Asset: "USDT", Decimals: decimals},
		ReceivedAmount: &commonv1.Money{MinorUnits: 100_000000, Asset: "USDT", Decimals: decimals},
		Fee:            &commonv1.Money{MinorUnits: 1_000000, Asset: "USDT", Decimals: decimals},
		QuoteId:        "quote-e2e",
		Rate:           "1.0",
		Txid:           "0xabc",
		StateVersion:   2,
		OccurredAt:     timestamppb.New(OccurredAt),
	}
	return marshal("payment.events", "payment.completed", eventID, paymentID, event)
}

// LedgerPosted builds a transaction.posted event with two entries.
func LedgerPosted(eventID, transactionID, refID string) (Event, error) {
	event := &eventsv1.LedgerEvent{
		TransactionId: transactionID,
		Type:          eventsv1.LedgerEventType_LEDGER_EVENT_TYPE_TRANSACTION_POSTED,
		RefType:       "payment",
		RefId:         refID,
		Entries: []*eventsv1.LedgerEntry{
			{
				AccountId: "acc-clearing",
				Direction: commonv1.Direction_DIRECTION_DEBIT,
				Amount:    &commonv1.Money{MinorUnits: 100_000000, Asset: "USDT", Decimals: 6},
			},
			{
				AccountId: "acc-merchant",
				Direction: commonv1.Direction_DIRECTION_CREDIT,
				Amount:    &commonv1.Money{MinorUnits: 100_000000, Asset: "USDT", Decimals: 6},
			},
		},
		OccurredAt: timestamppb.New(OccurredAt),
	}
	return marshal("ledger.events", "transaction.posted", eventID, "acc-clearing", event)
}

// PayoutRequested builds a payout.requested event.
func PayoutRequested(eventID, payoutID, merchantID string) (Event, error) {
	event := &eventsv1.PayoutEvent{
		PayoutId:     payoutID,
		MerchantId:   merchantID,
		Type:         eventsv1.PayoutEventType_PAYOUT_EVENT_TYPE_REQUESTED,
		Amount:       &commonv1.Money{MinorUnits: 50_000000, Asset: "USDT", Decimals: 6},
		Fee:          &commonv1.Money{MinorUnits: 500000, Asset: "USDT", Decimals: 6},
		Provider:     "mock",
		StateVersion: 1,
		OccurredAt:   timestamppb.New(OccurredAt),
	}
	return marshal("payout.events", "payout.requested", eventID, payoutID, event)
}

// ReconAmountMismatch builds an amount_mismatch alert.
func ReconAmountMismatch(eventID, refID, merchantID string) (Event, error) {
	event := &eventsv1.ReconAlert{
		AlertId:    eventID,
		Type:       eventsv1.ReconAlertType_RECON_ALERT_TYPE_AMOUNT_MISMATCH,
		RefId:      refID,
		Severity:   eventsv1.ReconAlertSeverity_RECON_ALERT_SEVERITY_WARNING,
		Expected:   "100000000",
		Actual:     "90000000",
		CreatedAt:  timestamppb.New(OccurredAt),
		MerchantId: merchantID,
	}
	return marshal("recon.alerts", "recon.alert", eventID, refID, event)
}

// ReserveCreated builds a reserve.created event without entries and without
// transaction_id (creates no fact rows; checks of the materialized view filters).
func ReserveCreated(eventID, refID string) (Event, error) {
	event := &eventsv1.LedgerEvent{
		Type:       eventsv1.LedgerEventType_LEDGER_EVENT_TYPE_RESERVE_CREATED,
		RefType:    "payout",
		RefId:      refID,
		OccurredAt: timestamppb.New(OccurredAt),
	}
	return marshal("ledger.events", "reserve.created", eventID, refID, event)
}

// LedgerUnspecifiedDirection builds a transaction.posted with an entry of
// direction UNSPECIFIED (does not map to Enum8, so no row appears).
func LedgerUnspecifiedDirection(eventID, transactionID, refID string) (Event, error) {
	event := &eventsv1.LedgerEvent{
		TransactionId: transactionID,
		Type:          eventsv1.LedgerEventType_LEDGER_EVENT_TYPE_TRANSACTION_POSTED,
		RefType:       "payment",
		RefId:         refID,
		Entries: []*eventsv1.LedgerEntry{{
			AccountId: "acc-unspecified",
			Direction: commonv1.Direction_DIRECTION_UNSPECIFIED,
			Amount:    &commonv1.Money{MinorUnits: 1, Asset: "USDT", Decimals: 6},
		}},
		OccurredAt: timestamppb.New(OccurredAt),
	}
	return marshal("ledger.events", "transaction.posted", eventID, refID, event)
}

// marshal serializes the event body and wraps it in an envelope.
func marshal(topic, eventType, eventID, key string, message proto.Message) (Event, error) {
	payload, err := proto.Marshal(message)
	if err != nil {
		return Event{}, fmt.Errorf("marshal %s: %w", eventType, err)
	}
	return NewEvent(topic, eventType, eventID, key, OccurredAt, payload), nil
}

// Publish publishes events synchronously (acks=all) and verifies delivery.
func Publish(ctx context.Context, brokers []string, events []Event) error {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.RecordRetries(3),
	)
	if err != nil {
		return fmt.Errorf("kafka client: %w", err)
	}
	defer client.Close()
	records := make([]*kgo.Record, 0, len(events))
	for _, event := range events {
		record := &kgo.Record{Topic: event.Topic, Key: []byte(event.Key), Value: event.Payload}
		for name, value := range event.Headers {
			record.Headers = append(record.Headers, kgo.RecordHeader{Key: name, Value: []byte(value)})
		}
		records = append(records, record)
	}
	results := client.ProduceSync(ctx, records...)
	if err := results.FirstErr(); err != nil {
		return fmt.Errorf("publish events: %w", err)
	}
	return nil
}
