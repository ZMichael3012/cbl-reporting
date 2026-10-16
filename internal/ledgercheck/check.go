// Package ledgercheck periodically reconciles Reporting facts with Ledger over the
// dim_accounts dictionary accounts: pipeline trustworthiness control, without money
// decisions and without publishing recon.alerts (Reconciler domain, T-09).
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

// Reconciliation discrepancy classes.
const (
	// KindMissingInReporting means a Ledger entry missing from the facts.
	KindMissingInReporting = "missing_in_reporting"
	// KindExtraInReporting means a fact row missing from Ledger.
	KindExtraInReporting = "extra_in_reporting"
	// KindAmountMismatch means a discrepancy in the number of entries or in amounts.
	KindAmountMismatch = "amount_mismatch"
)

// Ledger describes reading an account statement (implemented by internal/ledger).
type Ledger interface {
	ListEntries(ctx context.Context, accountID string, from, to time.Time, limit uint32, cursor string) ([]ledger.Entry, string, error)
}

// Discrepancy describes a single reconciliation discrepancy.
type Discrepancy struct {
	// Kind sets the discrepancy class.
	Kind string
	// TransactionID sets the transaction of the discrepancy.
	TransactionID string
	// Ledger sets the Ledger-side aggregate.
	Ledger storage.TxAggregate
	// Fact sets the fact-side aggregate.
	Fact storage.TxAggregate
}

// Aggregate converts Ledger entries into per-transaction aggregates; an unknown
// direction is an error (out-of-contract data does not silently distort amounts).
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

// mergeAggregates associatively merges a page aggregate into the common one (memory
// is bounded by the number of transactions in the window, not by the number of pages).
func mergeAggregates(destination, source map[string]storage.TxAggregate) {
	for transactionID, aggregate := range source {
		current := destination[transactionID]
		current.Count += aggregate.Count
		current.Debit += aggregate.Debit
		current.Credit += aggregate.Credit
		destination[transactionID] = current
	}
}

// Compare matches Ledger and fact aggregates; the discrepancy order is
// deterministic (class, transaction) for stable logs and tests.
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

// Summary describes the outcome of one reconciliation cycle.
type Summary struct {
	// Accounts sets the number of selected accounts.
	Accounts int
	// Checked sets the number of successfully reconciled accounts.
	Checked int
	// Missing sets the number of missing_in_reporting discrepancies.
	Missing int
	// Extra sets the number of extra_in_reporting discrepancies.
	Extra int
	// Mismatch sets the number of amount_mismatch discrepancies.
	Mismatch int
	// Truncated sets the number of skipped accounts that exceeded the page limit.
	Truncated int
	// Errors sets the number of accounts with a reconciliation error.
	Errors int
}

// Checker reconciles facts with Ledger over the dictionary accounts.
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

// New assembles the reconciler; a nil logger is replaced with a no-op one.
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

// Run executes a cycle immediately and then on the interval until the context is
// canceled; cycle errors are logged and do not terminate the worker.
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

// runCycle executes a reconciliation cycle and logs the result (context cancellation
// on service shutdown is not a failure and does not reach the error counters).
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

// Cycle executes one reconciliation cycle: selects the next portion of dictionary
// accounts (round-robin) and reconciles the window [now - 2W, now - W).
func (c *Checker) Cycle(ctx context.Context) (Summary, error) {
	now := time.Now().UTC()
	// Window boundaries are truncated to milliseconds (DateTime64(3) precision):
	// the same boundary on both sides excludes false discrepancies on the
	// sub-millisecond edge.
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

// accountResult describes the reconciliation outcome of a single account.
type accountResult struct {
	discrepancies []Discrepancy
	truncated     bool
}

// checkAccount reconciles a single account: Ledger pages (with per-page aggregation)
// against fact aggregates.
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

// accountsForCycle selects the next portion of dictionary accounts after the cursor
// with a wrap to the beginning (the walk eventually covers the whole dictionary).
// The cursor is not updated here: Cycle advances it by the accounts actually processed.
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

// advanceCursor advances the walk cursor: if no account of the portion was processed,
// the cursor stays in place (the portion repeats on the next cycle); partial failures
// do not block the walk (the cursor follows the last processed account); a completed
// lap starts anew.
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

// String describes the discrepancy for logs and tests.
func (d Discrepancy) String() string {
	return fmt.Sprintf("%s %s ledger=%+v fact=%+v", d.Kind, d.TransactionID, d.Ledger, d.Fact)
}
