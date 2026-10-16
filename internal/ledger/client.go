// Package ledger wraps the LedgerService gRPC client for the account dictionary
// and the reconciliation of Reporting with Ledger (CBL-11.3).
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

// Account describes a merchant account from LedgerService.ListMerchantAccounts.
type Account struct {
	// ID sets the account identifier.
	ID string
	// Type sets the chart account type (merchant or payout).
	Type string
	// Asset sets the account asset code.
	Asset string
	// Decimals sets the scale of the account asset.
	Decimals uint32
}

// Entry describes a ledger entry from LedgerService.ListEntries.
type Entry struct {
	// TransactionID sets the transaction identifier.
	TransactionID string
	// Direction sets the entry direction (debit or credit).
	Direction string
	// AmountMinor sets the amount in minor units.
	AmountMinor int64
}

// Client wraps the LedgerService gRPC client with a shared call timeout.
type Client struct {
	conn    *grpc.ClientConn
	svc     ledgerv1.LedgerServiceClient
	timeout time.Duration
}

// NewClient connects to LedgerService at the host:port address (plaintext; mTLS
// is task 0.6); the connection is lazy, service startup does not depend on Ledger.
func NewClient(addr string, timeout time.Duration) (*Client, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("dial ledger: %w", err)
	}
	return &Client{conn: conn, svc: ledgerv1.NewLedgerServiceClient(conn), timeout: timeout}, nil
}

// Close closes the gRPC connection.
func (c *Client) Close() error {
	return c.conn.Close()
}

// ListMerchantAccounts returns the merchant accounts with balances (the dictionary
// does not need balances and does not store them).
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

// ListEntries returns a page of account entries for the period [from, to) and
// the next page cursor (empty means there are no more pages).
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

// directionName maps the contract direction to the fact column name.
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
