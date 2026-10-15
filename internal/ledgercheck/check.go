// Пакет ledgercheck периодически сверяет факты Reporting с Ledger по счетам
// словаря dim_accounts: контроль достоверности конвейера, без денежных решений
// и без публикации recon.alerts (зона Reconciler, T-09).
package ledgercheck

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"time"

	"go.uber.org/zap"

	"github.com/cbl/cbl-reporting/internal/ledger"
	"github.com/cbl/cbl-reporting/internal/metrics"
	"github.com/cbl/cbl-reporting/internal/storage"
)

// Классы расхождений сверки.
const (
	// KindMissingInReporting означает проводку Ledger, отсутствующую в фактах.
	KindMissingInReporting = "missing_in_reporting"
	// KindExtraInReporting означает строку фактов, отсутствующую в Ledger.
	KindExtraInReporting = "extra_in_reporting"
	// KindAmountMismatch означает расхождение числа проводок или сумм.
	KindAmountMismatch = "amount_mismatch"
)

// Ledger описывает чтение выписки счета (реализуется internal/ledger).
type Ledger interface {
	ListEntries(ctx context.Context, accountID string, from, to time.Time, limit uint32, cursor string) ([]ledger.Entry, string, error)
}

// Discrepancy описывает одно расхождение сверки.
type Discrepancy struct {
	// Kind задает класс расхождения.
	Kind string
	// TransactionID задает транзакцию расхождения.
	TransactionID string
	// Ledger задает агрегат стороны Ledger.
	Ledger storage.TxAggregate
	// Fact задает агрегат стороны фактов.
	Fact storage.TxAggregate
}

// Aggregate переводит проводки Ledger в агрегаты по транзакциям; неизвестное
// направление - ошибка (данные вне контракта не искажают суммы молча).
func Aggregate(entries []ledger.Entry) (map[string]storage.TxAggregate, error) {
	aggregates := make(map[string]storage.TxAggregate)
	for _, entry := range entries {
		aggregate := aggregates[entry.TransactionID]
		aggregate.Count++
		switch entry.Direction {
		case "debit":
			aggregate.Debit += entry.AmountMinor
		case "credit":
			aggregate.Credit += entry.AmountMinor
		default:
			return nil, fmt.Errorf("unknown entry direction %q", entry.Direction)
		}
		aggregates[entry.TransactionID] = aggregate
	}
	return aggregates, nil
}

// mergeAggregates ассоциативно вливает агрегат страницы в общий (память
// ограничена числом транзакций окна, а не числом страниц).
func mergeAggregates(destination, source map[string]storage.TxAggregate) {
	for transactionID, aggregate := range source {
		current := destination[transactionID]
		current.Count += aggregate.Count
		current.Debit += aggregate.Debit
		current.Credit += aggregate.Credit
		destination[transactionID] = current
	}
}

// Compare сопоставляет агрегаты Ledger и фактов; порядок расхождений
// детерминирован (класс, транзакция) для стабильных логов и тестов.
func Compare(ledgerAgg, factAgg map[string]storage.TxAggregate) []Discrepancy {
	var discrepancies []Discrepancy
	for transactionID, ledgerValue := range ledgerAgg {
		factValue, ok := factAgg[transactionID]
		switch {
		case !ok:
			discrepancies = append(discrepancies, Discrepancy{
				Kind: KindMissingInReporting, TransactionID: transactionID, Ledger: ledgerValue,
			})
		case ledgerValue != factValue:
			discrepancies = append(discrepancies, Discrepancy{
				Kind: KindAmountMismatch, TransactionID: transactionID, Ledger: ledgerValue, Fact: factValue,
			})
		}
	}
	for transactionID, factValue := range factAgg {
		if _, ok := ledgerAgg[transactionID]; !ok {
			discrepancies = append(discrepancies, Discrepancy{
				Kind: KindExtraInReporting, TransactionID: transactionID, Fact: factValue,
			})
		}
	}
	sort.Slice(discrepancies, func(i, j int) bool {
		if discrepancies[i].Kind != discrepancies[j].Kind {
			return discrepancies[i].Kind < discrepancies[j].Kind
		}
		return discrepancies[i].TransactionID < discrepancies[j].TransactionID
	})
	return discrepancies
}

// Summary описывает итог одного цикла сверки.
type Summary struct {
	// Accounts задает число выбранных счетов.
	Accounts int
	// Checked задает число успешно сверенных счетов.
	Checked int
	// Missing задает число расхождений missing_in_reporting.
	Missing int
	// Extra задает число расхождений extra_in_reporting.
	Extra int
	// Mismatch задает число расхождений amount_mismatch.
	Mismatch int
	// Truncated задает число пропущенных счетов с превышением страниц.
	Truncated int
	// Errors задает число счетов с ошибкой сверки.
	Errors int
}

// Checker сверяет факты с Ledger по счетам словаря.
type Checker struct {
	db        *sql.DB
	ledger    Ledger
	interval  time.Duration
	window    time.Duration
	accounts  int
	pageLimit uint32
	maxPages  int
	log       *zap.Logger
	counters  *metrics.Counters
	cursor    string
}

// New собирает сверку; nil-логгер заменяется пустым.
func New(db *sql.DB, ledgerClient Ledger, interval, window time.Duration, accounts int, pageLimit uint32, maxPages int, log *zap.Logger, counters *metrics.Counters) (*Checker, error) {
	if db == nil || ledgerClient == nil {
		return nil, errors.New("db and ledger client are required")
	}
	if interval <= 0 || window <= 0 || accounts <= 0 || pageLimit == 0 || maxPages <= 0 {
		return nil, errors.New("check settings must be positive")
	}
	if log == nil {
		log = zap.NewNop()
	}
	if counters == nil {
		counters = &metrics.Counters{}
	}
	return &Checker{
		db:        db,
		ledger:    ledgerClient,
		interval:  interval,
		window:    window,
		accounts:  accounts,
		pageLimit: pageLimit,
		maxPages:  maxPages,
		log:       log,
		counters:  counters,
	}, nil
}

// Run выполняет цикл сразу и затем по интервалу до отмены контекста; ошибки
// цикла логируются и не завершают воркер.
func (c *Checker) Run(ctx context.Context) {
	c.runCycle(ctx)
	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.runCycle(ctx)
		}
	}
}

// runCycle выполняет цикл сверки и логирует результат (отмена контекста при
// остановке сервиса - не сбой и не попадает в счетчики ошибок).
func (c *Checker) runCycle(ctx context.Context) {
	summary, err := c.Cycle(ctx)
	if err != nil {
		if errors.Is(err, context.Canceled) || ctx.Err() != nil {
			c.log.Info("ledger check cycle canceled")
			return
		}
		c.counters.CheckErrors.Add(1)
		c.log.Warn("ledger check cycle failed", zap.Error(err))
		return
	}
	c.log.Info("ledger check cycle finished",
		zap.Int("accounts", summary.Accounts),
		zap.Int("checked", summary.Checked),
		zap.Int("missing_in_reporting", summary.Missing),
		zap.Int("extra_in_reporting", summary.Extra),
		zap.Int("amount_mismatch", summary.Mismatch),
		zap.Int("truncated", summary.Truncated),
		zap.Int("errors", summary.Errors))
}

// Cycle выполняет один цикл сверки: выбирает следующую порцию счетов словаря
// (round-robin) и сверяет окно [now - 2W, now - W).
func (c *Checker) Cycle(ctx context.Context) (Summary, error) {
	now := time.Now().UTC()
	// Границы окна выравниваются до миллисекунд (точность DateTime64(3)):
	// одинаковая граница на обеих сторонах исключает ложные расхождения
	// на субмиллисекундной кромке.
	from := now.Add(-2 * c.window).Truncate(time.Millisecond)
	to := now.Add(-c.window).Truncate(time.Millisecond)
	accountIDs, err := c.accountsForCycle(ctx)
	if err != nil {
		return Summary{}, err
	}
	summary := Summary{Accounts: len(accountIDs)}
	anyProcessed := false
	processed := ""
	for _, accountID := range accountIDs {
		if ctx.Err() != nil {
			return summary, ctx.Err()
		}
		result, err := c.checkAccount(ctx, accountID, from, to)
		if err != nil {
			summary.Errors++
			c.counters.CheckErrors.Add(1)
			c.log.Warn("ledger check account failed",
				zap.String("account_id", accountID), zap.Error(err))
			continue
		}
		anyProcessed = true
		processed = accountID
		if result.truncated {
			summary.Truncated++
			c.counters.CheckTruncated.Add(1)
			c.log.Warn("ledger check account skipped: ledger pages limit exceeded",
				zap.String("account_id", accountID))
			continue
		}
		summary.Checked++
		c.counters.CheckAccounts.Add(1)
		for _, discrepancy := range result.discrepancies {
			switch discrepancy.Kind {
			case KindMissingInReporting:
				summary.Missing++
				c.counters.CheckMissing.Add(1)
			case KindExtraInReporting:
				summary.Extra++
				c.counters.CheckExtra.Add(1)
			case KindAmountMismatch:
				summary.Mismatch++
				c.counters.CheckMismatch.Add(1)
			}
			c.log.Warn("ledger check discrepancy",
				zap.String("account_id", accountID),
				zap.String("kind", discrepancy.Kind),
				zap.String("transaction_id", discrepancy.TransactionID),
				zap.Int64("ledger_count", discrepancy.Ledger.Count),
				zap.Int64("fact_count", discrepancy.Fact.Count),
				zap.Int64("ledger_debit", discrepancy.Ledger.Debit),
				zap.Int64("fact_debit", discrepancy.Fact.Debit),
				zap.Int64("ledger_credit", discrepancy.Ledger.Credit),
				zap.Int64("fact_credit", discrepancy.Fact.Credit))
		}
	}
	c.advanceCursor(accountIDs, anyProcessed, processed)
	c.counters.CheckCycles.Add(1)
	return summary, nil
}

// accountResult описывает итог сверки одного счета.
type accountResult struct {
	discrepancies []Discrepancy
	truncated     bool
}

// checkAccount сверяет один счет: страницы Ledger (с постраничной агрегацией)
// против агрегатов фактов.
func (c *Checker) checkAccount(ctx context.Context, accountID string, from, to time.Time) (accountResult, error) {
	aggregates := make(map[string]storage.TxAggregate)
	cursor := ""
	for page := 0; ; page++ {
		items, next, err := c.ledger.ListEntries(ctx, accountID, from, to, c.pageLimit, cursor)
		if err != nil {
			return accountResult{}, err
		}
		pageAggregates, err := Aggregate(items)
		if err != nil {
			return accountResult{}, err
		}
		mergeAggregates(aggregates, pageAggregates)
		if next == "" {
			break
		}
		if page+1 >= c.maxPages {
			return accountResult{truncated: true}, nil
		}
		cursor = next
	}
	facts, err := storage.FactsAggregate(ctx, c.db, accountID, from, to)
	if err != nil {
		return accountResult{}, err
	}
	return accountResult{discrepancies: Compare(aggregates, facts)}, nil
}

// accountsForCycle выбирает следующую порцию счетов словаря после курсора с
// заворотом на начало (обход со временем покрывает весь словарь). Курсор не
// обновляется здесь: сдвиг выполняет Cycle по фактически обработанным счетам.
func (c *Checker) accountsForCycle(ctx context.Context) ([]string, error) {
	page, err := storage.DimAccountPage(ctx, c.db, c.cursor, c.accounts)
	if err != nil {
		return nil, err
	}
	seen := make(map[string]struct{}, len(page))
	for _, accountID := range page {
		seen[accountID] = struct{}{}
	}
	if len(page) < c.accounts && c.cursor != "" {
		rest, err := storage.DimAccountPage(ctx, c.db, "", c.accounts-len(page))
		if err != nil {
			return nil, err
		}
		for _, accountID := range rest {
			if _, ok := seen[accountID]; ok {
				continue
			}
			seen[accountID] = struct{}{}
			page = append(page, accountID)
		}
	}
	return page, nil
}

// advanceCursor двигает курсор обхода: если ни один счет порции не обработан,
// курсор остается на месте (порция повторится следующим циклом); частичные
// сбои не блокируют обход (курсор идет за последним обработанным счетом);
// завершенный круг начинается заново.
func (c *Checker) advanceCursor(accounts []string, anyProcessed bool, processed string) {
	if !anyProcessed || processed == "" {
		return
	}
	if len(accounts) == c.accounts {
		c.cursor = processed
		return
	}
	c.cursor = ""
}

// String описывает расхождение для логов и тестов.
func (d Discrepancy) String() string {
	return fmt.Sprintf("%s %s ledger=%+v fact=%+v", d.Kind, d.TransactionID, d.Ledger, d.Fact)
}
