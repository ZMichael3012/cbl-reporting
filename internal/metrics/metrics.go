// Package metrics contains the counters of the reporting service: dictionary
// population, backfill and Ledger reconciliation are exposed in the text
// Prometheus format on /metrics (full platform metrics are task 0.7).
package metrics

import (
	"fmt"
	"net/http"
	"sync/atomic"
)

// Counters describes the thread-safe counters of the service.
type Counters struct {
	// VocabularyAccounts counts inserted dictionary rows.
	VocabularyAccounts atomic.Int64
	// VocabularySkipped counts rejected Ledger rows (chart-of-accounts violation).
	VocabularySkipped atomic.Int64
	// VocabularyErrors counts errors of dictionary cycles.
	VocabularyErrors atomic.Int64
	// BackfillMutations counts launched backfill mutations.
	BackfillMutations atomic.Int64
	// BackfillErrors counts backfill errors.
	BackfillErrors atomic.Int64
	// CheckCycles counts completed reconciliation cycles.
	CheckCycles atomic.Int64
	// CheckAccounts counts successfully reconciled accounts.
	CheckAccounts atomic.Int64
	// CheckMissing counts missing_in_reporting discrepancies.
	CheckMissing atomic.Int64
	// CheckExtra counts extra_in_reporting discrepancies.
	CheckExtra atomic.Int64
	// CheckMismatch counts amount_mismatch discrepancies.
	CheckMismatch atomic.Int64
	// CheckTruncated counts skipped accounts that exceeded the page limit.
	CheckTruncated atomic.Int64
	// CheckErrors counts account reconciliation errors.
	CheckErrors atomic.Int64
}

// Handler exposes the counters in the text Prometheus format: the # TYPE header
// is printed once per family (name without labels), samples are printed with labels.
func (c *Counters) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		printed := make(map[string]struct{})
		for _, metric := range c.metrics() {
			if _, ok := printed[metric.name]; !ok {
				_, _ = fmt.Fprintf(w, "# TYPE %s counter\n", metric.name)
				printed[metric.name] = struct{}{}
			}
			_, _ = fmt.Fprintf(w, "%s%s %d\n", metric.name, metric.labels, metric.value)
		}
	})
}

// metric describes one output line (labels is a suffix of the form {kind="..."}).
type metric struct {
	name   string
	labels string
	value  int64
}

// metrics collects a snapshot of the counters (the read order is stable).
func (c *Counters) metrics() []metric {
	return []metric{
		{name: "reporting_vocabulary_accounts_total", value: c.VocabularyAccounts.Load()},
		{name: "reporting_vocabulary_skipped_total", value: c.VocabularySkipped.Load()},
		{name: "reporting_vocabulary_errors_total", value: c.VocabularyErrors.Load()},
		{name: "reporting_backfill_mutations_total", value: c.BackfillMutations.Load()},
		{name: "reporting_backfill_errors_total", value: c.BackfillErrors.Load()},
		{name: "reporting_ledger_check_cycles_total", value: c.CheckCycles.Load()},
		{name: "reporting_ledger_check_accounts_total", value: c.CheckAccounts.Load()},
		{name: "reporting_ledger_check_discrepancies_total", labels: `{kind="missing_in_reporting"}`, value: c.CheckMissing.Load()},
		{name: "reporting_ledger_check_discrepancies_total", labels: `{kind="extra_in_reporting"}`, value: c.CheckExtra.Load()},
		{name: "reporting_ledger_check_discrepancies_total", labels: `{kind="amount_mismatch"}`, value: c.CheckMismatch.Load()},
		{name: "reporting_ledger_check_truncated_total", value: c.CheckTruncated.Load()},
		{name: "reporting_ledger_check_errors_total", value: c.CheckErrors.Load()},
	}
}
