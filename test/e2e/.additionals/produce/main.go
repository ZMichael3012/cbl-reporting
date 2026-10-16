// produce publishes test CBL events for the reporting service e2e.
// Run from the service directory: go run ./test/e2e/produce -scenario full
// Scenarios: full - four events (payment/ledger/payout/recon), prints
// identifiers for checks; duplicate - a repeat of the payment event; broken -
// a broken message followed by a valid one (pipeline resilience check);
// sentinel - an event into the partition of the original one. The -live flag
// sets occurred_at to the current time (reporting check against Ledger in e2e).
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/cbl/cbl-reporting/test/producer"
)

// main parses the flags and runs the publish scenario.
func main() {
	brokerList := flag.String("brokers", "localhost:9092", "comma-separated list of brokers")
	scenario := flag.String("scenario", "full", "scenario: full, duplicate, broken, sentinel")
	paymentID := flag.String("payment-id", "", "payment_id for duplicate/broken/sentinel")
	eventID := flag.String("event-id", "", "event_id for duplicate")
	key := flag.String("key", "", "partition key for sentinel (payment_id of the original event)")
	live := flag.Bool("live", false, "set occurred_at to the current time (e2e reconciliation window)")
	flag.Parse()

	// The reporting check window counts from the current time: check scenarios
	// are published with occurred_at=now, the others - with a fixed time for
	// stable value checks.
	if *live {
		producer.OccurredAt = time.Now().UTC()
	}

	brokers := strings.Split(*brokerList, ",")
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	var events []producer.Event
	switch *scenario {
	case "full":
		transactionID := "tx-" + uuid.NewString()
		payoutID := "po-" + uuid.NewString()
		currentPaymentID := "pay-" + uuid.NewString()
		paymentEventID := uuid.NewString()
		alertEventID := uuid.NewString()
		payment, err := producer.PaymentCompleted(paymentEventID, currentPaymentID, "m-1")
		exitOnError(err)
		ledger, err := producer.LedgerPosted(uuid.NewString(), transactionID, currentPaymentID)
		exitOnError(err)
		payout, err := producer.PayoutRequested(uuid.NewString(), payoutID, "m-1")
		exitOnError(err)
		alert, err := producer.ReconAmountMismatch(alertEventID, payoutID, "m-1")
		exitOnError(err)
		events = []producer.Event{payment, ledger, payout, alert}
		fmt.Printf("payment_id=%s\npayment_event_id=%s\ntransaction_id=%s\npayout_id=%s\nalert_id=%s\noccurred_at=%s\n",
			currentPaymentID, paymentEventID, transactionID, payoutID, alertEventID,
			producer.OccurredAt.Format(time.RFC3339Nano))
	case "duplicate":
		if *paymentID == "" || *eventID == "" {
			exitOnError(fmt.Errorf("duplicate requires -payment-id and -event-id"))
		}
		payment, err := producer.PaymentCompleted(*eventID, *paymentID, "m-1")
		exitOnError(err)
		events = []producer.Event{payment}
	case "sentinel":
		// sentinel is published into the partition of the original event (same key):
		// waiting for sentinel proves that the earlier duplicate was processed.
		if *paymentID == "" || *key == "" {
			exitOnError(fmt.Errorf("sentinel requires -payment-id and -key"))
		}
		sentinel, err := producer.PaymentCompleted(uuid.NewString(), *paymentID, "m-1")
		exitOnError(err)
		sentinel.Key = *key
		events = []producer.Event{sentinel}
		fmt.Printf("sentinel_payment_id=%s\n", *paymentID)
	case "broken":
		if *paymentID == "" {
			exitOnError(fmt.Errorf("broken requires -payment-id"))
		}
		broken := producer.Event{Topic: "payment.events", Key: "broken", Headers: map[string]string{
			"event_id":   uuid.NewString(),
			"event_type": "payment.broken",
		}, Payload: []byte{0xff, 0xfe, 0xfd}}
		valid, err := producer.PaymentCompleted(uuid.NewString(), *paymentID, "m-1")
		exitOnError(err)
		events = []producer.Event{broken, valid}
		fmt.Printf("after_broken_payment_id=%s\n", *paymentID)
	default:
		exitOnError(fmt.Errorf("unknown scenario: %s", *scenario))
	}

	exitOnError(producer.Publish(ctx, brokers, events))
}

// exitOnError terminates the process with an error.
func exitOnError(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}
