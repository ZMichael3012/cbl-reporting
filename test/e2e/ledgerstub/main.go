// ledgerstub - e2e-сервис тестового Ledger для compose-стенда reporting:
// gRPC LedgerService (словарь и сверка) плюс административный HTTP для сида
// проводок (в продуктовый образ не входит).
package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/cbl/cbl-reporting/test/ledgerstub"
)

// entrySeed описывает проводку в теле административного запроса.
type entrySeed struct {
	TransactionID string `json:"transaction_id"`
	Direction     string `json:"direction"`
	AmountMinor   int64  `json:"amount_minor"`
	CreatedAt     string `json:"created_at"`
}

// entriesRequest описывает тело POST /entries.
type entriesRequest struct {
	AccountID string      `json:"account_id"`
	Entries   []entrySeed `json:"entries"`
}

// main поднимает gRPC-сервер и административный HTTP-сервер до сигнала.
func main() {
	grpcAddr := getenv("STUB_GRPC_ADDR", ":9040")
	httpAddr := getenv("STUB_HTTP_ADDR", ":9041")
	merchantRef := getenv("STUB_MERCHANT_REF", "m-1")

	server := ledgerstub.New()
	server.SeedAccounts(merchantRef,
		ledgerstub.Account{ID: "acc-merchant", Type: "merchant", Asset: "USDT", Decimals: 6},
		ledgerstub.Account{ID: "acc-payout", Type: "payout", Asset: "USDT", Decimals: 6},
	)
	addr, err := server.Start(grpcAddr)
	if err != nil {
		log.Fatal(err)
	}
	defer server.Stop()
	fmt.Println("ledger stub grpc started on", addr)

	mux := http.NewServeMux()
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = fmt.Fprintln(w, "ok")
	})
	mux.HandleFunc("/entries", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var request entriesRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			http.Error(w, "invalid json", http.StatusBadRequest)
			return
		}
		if request.AccountID == "" {
			http.Error(w, "account_id is required", http.StatusBadRequest)
			return
		}
		entries := make([]ledgerstub.Entry, 0, len(request.Entries))
		for _, seed := range request.Entries {
			createdAt, err := time.Parse(time.RFC3339Nano, seed.CreatedAt)
			if err != nil {
				http.Error(w, "invalid created_at", http.StatusBadRequest)
				return
			}
			entries = append(entries, ledgerstub.Entry{
				TransactionID: seed.TransactionID,
				Direction:     strings.ToLower(seed.Direction),
				AmountMinor:   seed.AmountMinor,
				CreatedAt:     createdAt,
			})
		}
		server.SeedEntries(request.AccountID, entries...)
		w.WriteHeader(http.StatusNoContent)
	})
	httpSrv := &http.Server{Addr: httpAddr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		fmt.Println("ledger stub admin started on", httpAddr)
		if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal(err)
		}
	}()

	wait := make(chan os.Signal, 1)
	signal.Notify(wait, os.Interrupt, syscall.SIGTERM)
	<-wait
	_ = httpSrv.Close()
}

// getenv возвращает переменную окружения или значение по умолчанию.
func getenv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
