// Пакет metrics содержит счетчики сервиса reporting: наполнение словаря,
// backfill и сверка с Ledger отдают текстовый Prometheus-формат на /metrics
// (полноценные метрики платформы - задача 0.7).
package metrics

import (
	"fmt"
	"net/http"
	"sync/atomic"
)

// Counters описывает потокобезопасные счетчики сервиса.
type Counters struct {
	// VocabularyAccounts считает вставленные строки словаря.
	VocabularyAccounts atomic.Int64
	// VocabularySkipped считает отклоненные строки Ledger (нарушение плана).
	VocabularySkipped atomic.Int64
	// VocabularyErrors считает ошибки циклов словаря.
	VocabularyErrors atomic.Int64
	// BackfillMutations считает запущенные мутации backfill.
	BackfillMutations atomic.Int64
	// BackfillErrors считает ошибки backfill.
	BackfillErrors atomic.Int64
	// CheckCycles считает завершенные циклы сверки.
	CheckCycles atomic.Int64
	// CheckAccounts считает успешно сверенные счета.
	CheckAccounts atomic.Int64
	// CheckMissing считает расхождения missing_in_reporting.
	CheckMissing atomic.Int64
	// CheckExtra считает расхождения extra_in_reporting.
	CheckExtra atomic.Int64
	// CheckMismatch считает расхождения amount_mismatch.
	CheckMismatch atomic.Int64
	// CheckTruncated считает пропущенные счета с превышением страниц.
	CheckTruncated atomic.Int64
	// CheckErrors считает ошибки сверки счетов.
	CheckErrors atomic.Int64
}

// Handler отдает счетчики в текстовом Prometheus-формате: заголовок # TYPE
// печатается один раз на семейство (имя без меток), образцы - с метками.
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

// metric описывает одну строку вывода (labels - суффикс вида {kind="..."}).
type metric struct {
	name   string
	labels string
	value  int64
}

// metrics собирает снимок счетчиков (read-порядок стабилен).
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
