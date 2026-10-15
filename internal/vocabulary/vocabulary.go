// Пакет vocabulary наполняет словарь reporting.dim_accounts (мерчантские счета
// из LedgerService, системные - деривацией плана счетов) и backfill-ит
// исторические строки фактов леджера (CBL-11.3).
package vocabulary

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"time"

	"go.uber.org/zap"

	"github.com/cbl/cbl-reporting/internal/domain"
	"github.com/cbl/cbl-reporting/internal/ledger"
	"github.com/cbl/cbl-reporting/internal/metrics"
	"github.com/cbl/cbl-reporting/internal/storage"
)

// Ledger описывает чтение счетов мерчанта (реализуется internal/ledger).
type Ledger interface {
	ListMerchantAccounts(ctx context.Context, merchantRef string) ([]ledger.Account, error)
}

// Syncer периодически наполняет словарь и запускает backfill истории.
type Syncer struct {
	db              *sql.DB
	ledger          Ledger
	refreshInterval time.Duration
	backfillLimit   int
	merchantsLimit  int
	merchantCursor  string
	log             *zap.Logger
	counters        *metrics.Counters
}

// New собирает воркер словаря; nil-логгер заменяется пустым.
func New(db *sql.DB, ledgerClient Ledger, refreshInterval time.Duration, backfillLimit, merchantsLimit int, log *zap.Logger, counters *metrics.Counters) (*Syncer, error) {
	if db == nil || ledgerClient == nil {
		return nil, errors.New("db and ledger client are required")
	}
	if refreshInterval <= 0 || backfillLimit <= 0 || merchantsLimit <= 0 {
		return nil, errors.New("refresh interval and limits must be positive")
	}
	if log == nil {
		log = zap.NewNop()
	}
	if counters == nil {
		counters = &metrics.Counters{}
	}
	return &Syncer{
		db:              db,
		ledger:          ledgerClient,
		refreshInterval: refreshInterval,
		backfillLimit:   backfillLimit,
		merchantsLimit:  merchantsLimit,
		log:             log,
		counters:        counters,
	}, nil
}

// Run выполняет цикл сразу и затем по интервалу до отмены контекста; ошибки
// цикла логируются и не завершают воркер (конвейер и readyz живут без словаря).
func (s *Syncer) Run(ctx context.Context) {
	s.runCycle(ctx)
	ticker := time.NewTicker(s.refreshInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.runCycle(ctx)
		}
	}
}

// runCycle выполняет цикл и логирует результат.
func (s *Syncer) runCycle(ctx context.Context) {
	inserted, mutations, err := s.Cycle(ctx)
	if err != nil {
		s.counters.VocabularyErrors.Add(1)
		s.log.Warn("vocabulary cycle failed", zap.Error(err))
		return
	}
	if inserted > 0 || mutations > 0 {
		s.log.Info("vocabulary refreshed",
			zap.Int("inserted", inserted),
			zap.Int("backfill_mutations", mutations))
	}
}

// Cycle выполняет один цикл: наполнение словаря и backfill истории.
func (s *Syncer) Cycle(ctx context.Context) (inserted, mutations int, err error) {
	inserted, err = s.refresh(ctx)
	if err != nil {
		return inserted, 0, err
	}
	mutations, err = s.backfill(ctx)
	if err != nil {
		return inserted, mutations, err
	}
	return inserted, mutations, nil
}

// refresh вносит недостающие строки словаря: мерчантские счета из Ledger,
// системные - производные от пар (asset, decimals) фактов леджера.
func (s *Syncer) refresh(ctx context.Context) (int, error) {
	merchants, err := s.merchantAccounts(ctx)
	if err != nil {
		return 0, err
	}
	system, err := s.platformAccounts(ctx)
	if err != nil {
		return 0, err
	}
	rows := append(merchants, system...)
	if err := storage.InsertDimAccounts(ctx, s.db, rows); err != nil {
		return 0, err
	}
	s.counters.VocabularyAccounts.Add(int64(len(rows)))
	return len(rows), nil
}

// merchantAccounts собирает строки словаря для мерчантов, чье требуемое
// покрытие из фактов не выполнено (новый мерчант, новый актив известного
// мерчанта, новый payout-счет): у Ledger запрашиваются все счета мерчанта, в
// словарь вносятся отсутствующие и изменившиеся строки.
func (s *Syncer) merchantAccounts(ctx context.Context) ([]storage.DimAccount, error) {
	pairs, err := storage.FactMerchantPairs(ctx, s.db)
	if err != nil {
		return nil, err
	}
	if len(pairs) == 0 {
		return nil, nil
	}
	coverage, err := storage.DimPairCoverage(ctx, s.db)
	if err != nil {
		return nil, err
	}
	needed := make(map[string]struct{})
	for _, pair := range pairs {
		if _, ok := coverage[pair]; ok {
			continue
		}
		if !domain.ValidMerchantRef(pair.MerchantID) {
			s.counters.VocabularySkipped.Add(1)
			s.log.Warn("unexpected merchant id in facts", zap.String("merchant_id", pair.MerchantID))
			continue
		}
		needed[pair.MerchantID] = struct{}{}
	}
	merchants := make([]string, 0, len(needed))
	for merchantRef := range needed {
		merchants = append(merchants, merchantRef)
	}
	sort.Strings(merchants)
	// Бюджет цикла с ротацией: хвост больших наборов не голодает (курсор
	// двигается по кругу, покрытие пересчитывается каждый цикл).
	merchants, s.merchantCursor = rotateBudget(merchants, s.merchantCursor, s.merchantsLimit)

	var rows []storage.DimAccount
	for _, merchantRef := range merchants {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		accounts, err := s.ledger.ListMerchantAccounts(ctx, merchantRef)
		if err != nil {
			return nil, fmt.Errorf("list merchant accounts %q: %w", merchantRef, err)
		}
		candidates := make([]storage.DimAccount, 0, len(accounts))
		for _, account := range accounts {
			row, ok := dimRow(account.ID, account.Type, merchantRef, account.Asset, account.Decimals)
			if !ok {
				s.counters.VocabularySkipped.Add(1)
				s.log.Warn("ledger account outside account plan",
					zap.String("account_id", account.ID), zap.String("type", account.Type))
				continue
			}
			candidates = append(candidates, row)
		}
		changed, err := filterChanged(ctx, s.db, candidates)
		if err != nil {
			return nil, err
		}
		rows = append(rows, changed...)
	}
	return rows, nil
}

// filterChanged оставляет строки, отсутствующие в словаре или отличающиеся от
// сохраненных (diff по account_id).
func filterChanged(ctx context.Context, db *sql.DB, candidates []storage.DimAccount) ([]storage.DimAccount, error) {
	if len(candidates) == 0 {
		return nil, nil
	}
	ids := make([]string, 0, len(candidates))
	for _, candidate := range candidates {
		ids = append(ids, candidate.AccountID)
	}
	existing, err := storage.DimAccountsByIDs(ctx, db, ids)
	if err != nil {
		return nil, err
	}
	var changed []storage.DimAccount
	for _, candidate := range candidates {
		if current, ok := existing[candidate.AccountID]; ok && current == candidate {
			continue
		}
		changed = append(changed, candidate)
	}
	return changed, nil
}

// platformAccounts добавляет производные системные счета для пар (asset,
// decimals), встреченных в фактах леджера (деривация CBL-003, раздел 9).
func (s *Syncer) platformAccounts(ctx context.Context) ([]storage.DimAccount, error) {
	pairs, err := storage.FactAssetPairs(ctx, s.db)
	if err != nil {
		return nil, err
	}
	if len(pairs) == 0 {
		return nil, nil
	}
	candidates := make([]storage.DimAccount, 0, len(pairs)*len(domain.PlatformAccountTypes))
	for _, pair := range pairs {
		for _, accountType := range domain.PlatformAccountTypes {
			accountID, deriveErr := domain.DeriveAccountID(accountType, pair.Asset, pair.Decimals, "")
			if deriveErr != nil {
				s.counters.VocabularySkipped.Add(1)
				s.log.Warn("derive platform account failed",
					zap.String("asset", pair.Asset), zap.Uint8("decimals", pair.Decimals), zap.Error(deriveErr))
				continue
			}
			candidates = append(candidates, storage.DimAccount{
				AccountID:   accountID,
				AccountType: accountType,
				Asset:       pair.Asset,
				Decimals:    pair.Decimals,
			})
		}
	}
	return filterMissing(ctx, s.db, candidates)
}

// filterMissing оставляет кандидатов, отсутствующих в словаре.
func filterMissing(ctx context.Context, db *sql.DB, candidates []storage.DimAccount) ([]storage.DimAccount, error) {
	if len(candidates) == 0 {
		return nil, nil
	}
	ids := make([]string, 0, len(candidates))
	for _, candidate := range candidates {
		ids = append(ids, candidate.AccountID)
	}
	existing, err := storage.DimExistingIDs(ctx, db, ids)
	if err != nil {
		return nil, err
	}
	var missing []storage.DimAccount
	for _, candidate := range candidates {
		if _, ok := existing[candidate.AccountID]; ok {
			continue
		}
		missing = append(missing, candidate)
	}
	return missing, nil
}

// backfill обогащает исторические строки фактов леджера значениями словаря.
func (s *Syncer) backfill(ctx context.Context) (int, error) {
	unenriched, err := storage.FactUnenrichedAccounts(ctx, s.db, s.backfillLimit)
	if err != nil {
		return 0, err
	}
	if len(unenriched) == 0 {
		return 0, nil
	}
	accounts, err := storage.DimAccountsByIDs(ctx, s.db, unenriched)
	if err != nil {
		return 0, err
	}
	mutations := 0
	for _, accountID := range unenriched {
		account, ok := accounts[accountID]
		if !ok {
			continue
		}
		if err := storage.BackfillAccount(ctx, s.db, account); err != nil {
			s.counters.BackfillErrors.Add(1)
			s.log.Warn("backfill mutation failed",
				zap.String("account_id", accountID), zap.Error(err))
			continue
		}
		mutations++
		s.counters.BackfillMutations.Add(1)
	}
	return mutations, nil
}

// rotateBudget выбирает до limit элементов начиная после курсора с заворотом
// (пустой курсор или неполный набор - все элементы, курсор сбрасывается);
// возвращает выбранное и новый курсор.
func rotateBudget(sorted []string, cursor string, limit int) ([]string, string) {
	if len(sorted) <= limit {
		return sorted, ""
	}
	start := 0
	for index, item := range sorted {
		if item > cursor {
			start = index
			break
		}
	}
	chosen := make([]string, 0, limit)
	for offset := 0; offset < limit; offset++ {
		chosen = append(chosen, sorted[(start+offset)%len(sorted)])
	}
	return chosen, chosen[len(chosen)-1]
}

// dimRow валидирует строку словаря по правилам плана счетов (форма
// идентификатора, шкала Ledger до сужения до UInt8, тип и ссылка мерчанта).
func dimRow(accountID, accountType, merchantRef, asset string, decimals uint32) (storage.DimAccount, bool) {
	if !domain.ValidAccountID(accountID) || !domain.ValidAsset(asset) || decimals > 18 {
		return storage.DimAccount{}, false
	}
	if accountType != "merchant" && accountType != "payout" {
		return storage.DimAccount{}, false
	}
	if !domain.ValidMerchantRef(merchantRef) {
		return storage.DimAccount{}, false
	}
	return storage.DimAccount{
		AccountID:   accountID,
		AccountType: accountType,
		MerchantID:  merchantRef,
		Asset:       asset,
		Decimals:    uint8(decimals), //nolint:gosec // проверено выше: decimals <= 18
	}, true
}
