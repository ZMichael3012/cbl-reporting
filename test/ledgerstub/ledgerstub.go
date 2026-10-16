// Package ledgerstub starts a test LedgerService (gRPC) with network
// fixtures: it is used by the reporting vocabulary and ledger check integration
// tests and by the service e2e stand (not part of production code).
package ledgerstub

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sort"
	"strconv"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/cbl/cbl-protos/gen/go/billing/common/v1"
	ledgerv1 "github.com/cbl/cbl-protos/gen/go/billing/ledger/v1"
)

// Account describes a fixture merchant account.
type Account struct {
	// ID sets the account identifier.
	ID string
	// Type sets the account type (merchant or payout).
	Type string
	// Asset sets the asset code.
	Asset string
	// Decimals sets the asset scale.
	Decimals uint32
}

// Entry describes a fixture ledger entry.
type Entry struct {
	// TransactionID sets the transaction identifier.
	TransactionID string
	// Direction sets the direction (debit or credit).
	Direction string
	// AmountMinor sets the amount in minor units.
	AmountMinor int64
	// CreatedAt sets the entry time (period filter and check window).
	CreatedAt time.Time
}

// Server describes the test LedgerService.
type Server struct {
	ledgerv1.UnimplementedLedgerServiceServer

	mu       sync.Mutex
	accounts map[string][]Account
	entries  map[string][]Entry
	listener net.Listener
	grpc     *grpc.Server
}

// New builds an empty test server.
func New() *Server {
	return &Server{
		accounts: make(map[string][]Account),
		entries:  make(map[string][]Entry),
	}
}

// SeedAccounts sets the merchant accounts (a repeated call replaces the set).
func (s *Server) SeedAccounts(merchantRef string, accounts ...Account) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.accounts[merchantRef] = append([]Account(nil), accounts...)
}

// SeedEntries sets the account entries (a repeated call replaces the set).
func (s *Server) SeedEntries(accountID string, entries ...Entry) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.entries[accountID] = append([]Entry(nil), entries...)
}

// Start starts the gRPC server on the address (empty - a random localhost port)
// and returns the actual address to connect to.
func (s *Server) Start(addr string) (string, error) {
	if addr == "" {
		addr = "127.0.0.1:0"
	}
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return "", fmt.Errorf("listen: %w", err)
	}
	s.mu.Lock()
	s.listener = listener
	grpcSrv := grpc.NewServer()
	s.grpc = grpcSrv
	s.mu.Unlock()
	ledgerv1.RegisterLedgerServiceServer(grpcSrv, s)
	go func() { _ = grpcSrv.Serve(listener) }()
	return listener.Addr().String(), nil
}

// Stop stops the server and releases the port.
func (s *Server) Stop() {
	s.mu.Lock()
	grpcSrv, listener := s.grpc, s.listener
	s.grpc, s.listener = nil, nil
	s.mu.Unlock()
	if grpcSrv != nil {
		grpcSrv.Stop()
	}
	if listener != nil {
		_ = listener.Close()
	}
}

// ListMerchantAccounts returns the fixture merchant accounts.
func (s *Server) ListMerchantAccounts(_ context.Context, req *ledgerv1.ListMerchantAccountsRequest) (*ledgerv1.ListMerchantAccountsResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	accounts := s.accounts[req.GetMerchantRef()]
	response := &ledgerv1.ListMerchantAccountsResponse{}
	for _, account := range accounts {
		response.Accounts = append(response.Accounts, &ledgerv1.MerchantAccount{
			AccountId: account.ID,
			Type:      account.Type,
			Asset:     account.Asset,
			Decimals:  account.Decimals,
			Available: &commonv1.Money{MinorUnits: 0, Asset: account.Asset, Decimals: account.Decimals},
			Reserved:  &commonv1.Money{MinorUnits: 0, Asset: account.Asset, Decimals: account.Decimals},
		})
	}
	return response, nil
}

// ListEntries returns a page of account entries for the period [from, to) with
// cursor pagination (the cursor is an offset in a stable order).
func (s *Server) ListEntries(_ context.Context, req *ledgerv1.ListEntriesRequest) (*ledgerv1.ListEntriesResponse, error) {
	s.mu.Lock()
	fixtures := append([]Entry(nil), s.entries[req.GetAccountId()]...)
	s.mu.Unlock()

	from := time.Time{}
	if ts := req.GetPeriodFrom(); ts != nil {
		from = ts.AsTime()
	}
	to := time.Time{}
	if ts := req.GetPeriodTo(); ts != nil {
		to = ts.AsTime()
	}
	filtered := make([]Entry, 0, len(fixtures))
	for _, entry := range fixtures {
		if !from.IsZero() && entry.CreatedAt.Before(from) {
			continue
		}
		if !to.IsZero() && !entry.CreatedAt.Before(to) {
			continue
		}
		filtered = append(filtered, entry)
	}
	sort.Slice(filtered, func(i, j int) bool {
		if !filtered[i].CreatedAt.Equal(filtered[j].CreatedAt) {
			return filtered[i].CreatedAt.Before(filtered[j].CreatedAt)
		}
		return filtered[i].TransactionID < filtered[j].TransactionID
	})

	offset := 0
	if req.GetCursor() != "" {
		parsed, err := strconv.Atoi(req.GetCursor())
		if err != nil || parsed < 0 {
			return nil, errors.New("invalid cursor")
		}
		offset = parsed
	}
	if offset > len(filtered) {
		offset = len(filtered)
	}
	limit := int(req.GetLimit())
	if limit <= 0 {
		limit = 1000
	}
	end := offset + limit
	if end > len(filtered) {
		end = len(filtered)
	}
	response := &ledgerv1.ListEntriesResponse{}
	for _, entry := range filtered[offset:end] {
		direction, err := directionValue(entry.Direction)
		if err != nil {
			return nil, err
		}
		response.Entries = append(response.Entries, &ledgerv1.Entry{
			TransactionId: entry.TransactionID,
			AccountId:     req.GetAccountId(),
			Direction:     direction,
			Amount:        &commonv1.Money{MinorUnits: entry.AmountMinor},
			CreatedAt:     timestamppb.New(entry.CreatedAt),
		})
	}
	if end < len(filtered) {
		response.NextCursor = strconv.Itoa(end)
	}
	return response, nil
}

// directionValue maps a fixture direction name to the contract value.
func directionValue(direction string) (commonv1.Direction, error) {
	switch direction {
	case "debit":
		return commonv1.Direction_DIRECTION_DEBIT, nil
	case "credit":
		return commonv1.Direction_DIRECTION_CREDIT, nil
	default:
		return commonv1.Direction_DIRECTION_UNSPECIFIED, fmt.Errorf("unknown direction %q", direction)
	}
}
