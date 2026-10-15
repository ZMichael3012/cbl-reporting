// produce публикует тестовые события CBL для e2e сервиса reporting.
// Запуск из каталога сервиса: go run ./test/e2e/produce -scenario full
// Сценарии: full - четыре события (payment/ledger/payout/recon), печатает
// идентификаторы для проверок; duplicate - повтор события payment; broken -
// битое сообщение, затем валидное (проверка устойчивости конвейера).
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

// main разбирает флаги и выполняет сценарий публикации.
func main() {
	brokerList := flag.String("brokers", "localhost:9092", "список брокеров через запятую")
	scenario := flag.String("scenario", "full", "сценарий: full, duplicate, broken")
	paymentID := flag.String("payment-id", "", "payment_id для duplicate/broken/sentinel")
	eventID := flag.String("event-id", "", "event_id для duplicate")
	key := flag.String("key", "", "ключ партиции для sentinel (payment_id исходного события)")
	flag.Parse()

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
		fmt.Printf("payment_id=%s\npayment_event_id=%s\ntransaction_id=%s\npayout_id=%s\nalert_id=%s\n",
			currentPaymentID, paymentEventID, transactionID, payoutID, alertEventID)
	case "duplicate":
		if *paymentID == "" || *eventID == "" {
			exitOnError(fmt.Errorf("duplicate требует -payment-id и -event-id"))
		}
		payment, err := producer.PaymentCompleted(*eventID, *paymentID, "m-1")
		exitOnError(err)
		events = []producer.Event{payment}
	case "sentinel":
		// sentinel публикуется в партицию исходного события (тот же ключ):
		// ожидание sentinel доказывает обработку более раннего дубля.
		if *paymentID == "" || *key == "" {
			exitOnError(fmt.Errorf("sentinel требует -payment-id и -key"))
		}
		sentinel, err := producer.PaymentCompleted(uuid.NewString(), *paymentID, "m-1")
		exitOnError(err)
		sentinel.Key = *key
		events = []producer.Event{sentinel}
		fmt.Printf("sentinel_payment_id=%s\n", *paymentID)
	case "broken":
		if *paymentID == "" {
			exitOnError(fmt.Errorf("broken требует -payment-id"))
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
		exitOnError(fmt.Errorf("неизвестный сценарий: %s", *scenario))
	}

	exitOnError(producer.Publish(ctx, brokers, events))
}

// exitOnError завершает процесс с ошибкой.
func exitOnError(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}
