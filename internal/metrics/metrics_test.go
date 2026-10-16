package metrics

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestCountersHandler verifies the text Prometheus format of the counters.
func TestCountersHandler(t *testing.T) {
	counters := &Counters{}
	counters.VocabularyAccounts.Add(2)
	counters.CheckCycles.Add(3)
	counters.CheckMissing.Add(1)
	request := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	recorder := httptest.NewRecorder()
	counters.Handler().ServeHTTP(recorder, request)

	assert.Equal(t, http.StatusOK, recorder.Code)
	assert.Contains(t, recorder.Header().Get("Content-Type"), "text/plain")
	body := recorder.Body.String()
	for _, fragment := range []string{
		"# TYPE reporting_vocabulary_accounts_total counter",
		"reporting_vocabulary_accounts_total 2",
		"reporting_ledger_check_cycles_total 3",
		`reporting_ledger_check_discrepancies_total{kind="missing_in_reporting"} 1`,
		`reporting_ledger_check_discrepancies_total{kind="extra_in_reporting"} 0`,
		"reporting_ledger_check_errors_total 0",
	} {
		assert.Contains(t, body, fragment)
	}
	// Prometheus format: the name in # TYPE carries no labels, the family is declared once.
	typeLines := 0
	for _, line := range strings.Split(body, "\n") {
		if strings.HasPrefix(line, "# TYPE reporting_ledger_check_discrepancies_total ") {
			typeLines++
		}
		if strings.HasPrefix(line, "# TYPE") {
			assert.NotContains(t, line, "{", "labels are not allowed in a TYPE line")
		}
	}
	assert.Equal(t, 1, typeLines, "the discrepancy family is declared once")
}

// TestCountersZero verifies the zero snapshot without accesses.
func TestCountersZero(t *testing.T) {
	counters := &Counters{}
	request := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	recorder := httptest.NewRecorder()
	counters.Handler().ServeHTTP(recorder, request)
	assert.Contains(t, recorder.Body.String(), "reporting_backfill_mutations_total 0")
}
