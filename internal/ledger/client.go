// Пакет ledger оборачивает gRPC-клиент LedgerService для словаря счетов и
// сверки Reporting с Ledger (CBL-11.3).
package ledger

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/timestamppb"

	ledgerv1 "github.com/cbl/cbl-protos/gen/go/billing/ledger/v1"

	commonv1 "github.com/cbl/cbl-protos/gen/go/billing/common/v1"
)

// Account описывает счет мерчанта из LedgerService.ListMerchantAccounts.
type Account struct {
	// ID задает идентификатор счета.
	ID string
	// Type задает тип счета плана (merchant или payout).
	Type string
	// Asset задает код актива счета.
	Asset string
	// Decimals задает шкалу актива счета.
	Decimals uint32
}

// Entry описывает проводку из LedgerService.ListEntries.
type Entry struct {
	// TransactionID задает идентификатор транзакции.
	TransactionID string
	// Direction задает направление проводки (debit или credit).
	Direction string
	// AmountMinor задает сумму в минимальных единицах.
	AmountMinor int64
}

// Client оборачивает gRPC-клиент LedgerService с общим таймаутом вызовов.
type Client struct {
	conn    *grpc.ClientConn
	svc     ledgerv1.LedgerServiceClient
	timeout time.Duration
}

// NewClient подключается к LedgerService по адресу host:port (plaintext; mTLS -
// задача 0.6); соединение ленивое, старт сервиса от Ledger не зависит.
func NewClient(addr string, timeout time.Duration) (*Client, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("dial ledger: %w", err)
	}
	return &Client{conn: conn, svc: ledgerv1.NewLedgerServiceClient(conn), timeout: timeout}, nil
}

// Close закрывает gRPC-соединение.
func (c *Client) Close() error {
	return c.conn.Close()
}

// ListMerchantAccounts возвращает счета мерчанта с балансами (балансы словарю
// не нужны и не сохраняются).
func (c *Client) ListMerchantAccounts(ctx context.Context, merchantRef string) ([]Account, error) {
	callCtx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	response, err := c.svc.ListMerchantAccounts(callCtx, &ledgerv1.ListMerchantAccountsRequest{
		MerchantRef: merchantRef,
	})
	if err != nil {
		return nil, fmt.Errorf("list merchant accounts %q: %w", merchantRef, err)
	}
	accounts := make([]Account, 0, len(response.GetAccounts()))
	for _, account := range response.GetAccounts() {
		accounts = append(accounts, Account{
			ID:       account.GetAccountId(),
			Type:     account.GetType(),
			Asset:    account.GetAsset(),
			Decimals: account.GetDecimals(),
		})
	}
	return accounts, nil
}

// ListEntries возвращает страницу проводок счета за период [from, to) и
// курсор следующей страницы (пустой - страниц больше нет).
func (c *Client) ListEntries(ctx context.Context, accountID string, from, to time.Time, limit uint32, cursor string) ([]Entry, string, error) {
	callCtx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	response, err := c.svc.ListEntries(callCtx, &ledgerv1.ListEntriesRequest{
		AccountId:  accountID,
		Limit:      limit,
		Cursor:     cursor,
		PeriodFrom: timestamppb.New(from),
		PeriodTo:   timestamppb.New(to),
	})
	if err != nil {
		return nil, "", fmt.Errorf("list entries %q: %w", accountID, err)
	}
	entries := make([]Entry, 0, len(response.GetEntries()))
	for _, entry := range response.GetEntries() {
		direction, err := directionName(entry.GetDirection())
		if err != nil {
			return nil, "", fmt.Errorf("entry %s: %w", entry.GetTransactionId(), err)
		}
		entries = append(entries, Entry{
			TransactionID: entry.GetTransactionId(),
			Direction:     direction,
			AmountMinor:   entry.GetAmount().GetMinorUnits(),
		})
	}
	return entries, response.GetNextCursor(), nil
}

// directionName переводит направление контракта в имя колонки фактов.
func directionName(direction commonv1.Direction) (string, error) {
	switch direction {
	case commonv1.Direction_DIRECTION_DEBIT:
		return "debit", nil
	case commonv1.Direction_DIRECTION_CREDIT:
		return "credit", nil
	default:
		return "", fmt.Errorf("unknown entry direction %d", direction)
	}
}
