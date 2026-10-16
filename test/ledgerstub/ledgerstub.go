// Пакет ledgerstub поднимает тестовый LedgerService (gRPC) с сетевыми
// фикстурами: используется интеграционными тестами словаря и сверки reporting
// и e2e-стендом сервиса (в продуктовый код не входит).
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

// Account описывает счет мерчанта фикстуры.
type Account struct {
	// ID задает идентификатор счета.
	ID string
	// Type задает тип счета (merchant или payout).
	Type string
	// Asset задает код актива.
	Asset string
	// Decimals задает шкалу актива.
	Decimals uint32
}

// Entry описывает проводку фикстуры.
type Entry struct {
	// TransactionID задает идентификатор транзакции.
	TransactionID string
	// Direction задает направление (debit или credit).
	Direction string
	// AmountMinor задает сумму в минимальных единицах.
	AmountMinor int64
	// CreatedAt задает время проводки (фильтр периода и окно сверки).
	CreatedAt time.Time
}

// Server описывает тестовый LedgerService.
type Server struct {
	ledgerv1.UnimplementedLedgerServiceServer

	mu       sync.Mutex
	accounts map[string][]Account
	entries  map[string][]Entry
	listener net.Listener
	grpc     *grpc.Server
}

// New собирает пустой тестовый сервер.
func New() *Server {
	return &Server{
		accounts: make(map[string][]Account),
		entries:  make(map[string][]Entry),
	}
}

// SeedAccounts задает счета мерчанта (повторный вызов заменяет набор).
func (s *Server) SeedAccounts(merchantRef string, accounts ...Account) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.accounts[merchantRef] = append([]Account(nil), accounts...)
}

// SeedEntries задает проводки счета (повторный вызов заменяет набор).
func (s *Server) SeedEntries(accountID string, entries ...Entry) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.entries[accountID] = append([]Entry(nil), entries...)
}

// Start поднимает gRPC-сервер на адресе (пусто - случайный порт localhost) и
// возвращает фактический адрес для подключения.
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

// Stop останавливает сервер и освобождает порт.
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

// ListMerchantAccounts возвращает счета мерчанта по фикстурам.
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

// ListEntries возвращает страницу проводок счета за период [from, to) с
// курсорной пагинацией (курсор - смещение в стабильном порядке).
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

// directionValue переводит имя направления фикстуры в значение контракта.
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
