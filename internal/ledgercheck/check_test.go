package ledgercheck

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cbl/cbl-reporting/internal/ledger"
	"github.com/cbl/cbl-reporting/internal/storage"
)

// TestAggregate verifies aggregation of Ledger entries by transaction.
func TestAggregate(t *testing.T) {
	entries := []ledger.Entry{
		{TransactionID: "tx-1", Direction: "debit", AmountMinor: 100},
		{TransactionID: "tx-1", Direction: "credit", AmountMinor: 60},
		{TransactionID: "tx-1", Direction: "credit", AmountMinor: 40},
		{TransactionID: "tx-2", Direction: "credit", AmountMinor: 7},
	}
	got, err := Aggregate(entries)
	require.NoError(t, err)
	require.Len(t, got, 2)
	assert.Equal(t, storage.TxAggregate{Count: 3, Debit: 100, Credit: 100}, got["tx-1"])
	assert.Equal(t, storage.TxAggregate{Count: 1, Credit: 7}, got["tx-2"])
}

// TestAggregateEmpty verifies an empty input.
func TestAggregateEmpty(t *testing.T) {
	got, err := Aggregate(nil)
	require.NoError(t, err)
	assert.Empty(t, got)
}

// TestAggregateUnknownDirection verifies rejection of an out-of-contract direction.
func TestAggregateUnknownDirection(t *testing.T) {
	_, err := Aggregate([]ledger.Entry{{TransactionID: "tx-1", Direction: "sideways", AmountMinor: 1}})
	require.Error(t, err)
}

// TestMergeAggregates verifies associativity of page merging.
func TestMergeAggregates(t *testing.T) {
	destination := map[string]storage.TxAggregate{"tx-1": {Count: 1, Debit: 10}}
	mergeAggregates(destination, map[string]storage.TxAggregate{
		"tx-1": {Count: 2, Credit: 10},
		"tx-2": {Count: 1, Debit: 1},
	})
	assert.Equal(t, storage.TxAggregate{Count: 3, Debit: 10, Credit: 10}, destination["tx-1"])
	assert.Equal(t, storage.TxAggregate{Count: 1, Debit: 1}, destination["tx-2"])
}

// TestAdvanceCursor verifies the walk cursor semantics: a full failure of a portion
// does not move the cursor, a partial failure does not block the walk, a completed
// lap starts anew.
func TestAdvanceCursor(t *testing.T) {
	full := &Checker{accounts: 2, cursor: "keep"}
	full.advanceCursor([]string{"a", "b"}, false, "")
	assert.Equal(t, "keep", full.cursor, "no account was processed")

	partial := &Checker{accounts: 3}
	partial.advanceCursor([]string{"a", "b", "c"}, true, "c")
	assert.Equal(t, "c", partial.cursor, "partial failures do not block the walk")

	lap := &Checker{accounts: 5, cursor: "x"}
	lap.advanceCursor([]string{"a", "b"}, true, "b")
	assert.Empty(t, lap.cursor, "a completed lap starts anew")
}

// TestCompare verifies all discrepancy classes and a match.
func TestCompare(t *testing.T) {
	ledgerAgg := map[string]storage.TxAggregate{
		"tx-match":    {Count: 2, Debit: 100, Credit: 100},
		"tx-missing":  {Count: 1, Debit: 5},
		"tx-mismatch": {Count: 2, Debit: 100, Credit: 100},
	}
	factAgg := map[string]storage.TxAggregate{
		"tx-match":    {Count: 2, Debit: 100, Credit: 100},
		"tx-mismatch": {Count: 1, Debit: 100},
		"tx-extra":    {Count: 1, Credit: 9},
	}
	discrepancies := Compare(ledgerAgg, factAgg)
	require.Len(t, discrepancies, 3)
	// The order is deterministic: class first, then transaction.
	assert.Equal(t, KindAmountMismatch, discrepancies[0].Kind)
	assert.Equal(t, "tx-mismatch", discrepancies[0].TransactionID)
	assert.Equal(t, KindExtraInReporting, discrepancies[1].Kind)
	assert.Equal(t, "tx-extra", discrepancies[1].TransactionID)
	assert.Equal(t, KindMissingInReporting, discrepancies[2].Kind)
	assert.Equal(t, "tx-missing", discrepancies[2].TransactionID)
}

// TestCompareClean verifies the absence of discrepancies on matching data.
func TestCompareClean(t *testing.T) {
	agg := map[string]storage.TxAggregate{"tx-1": {Count: 1, Debit: 1}}
	assert.Empty(t, Compare(agg, map[string]storage.TxAggregate{"tx-1": {Count: 1, Debit: 1}}))
}

// TestCompareMismatchOnCount verifies a discrepancy in the number of entries with
// equal amounts (a missing counter-entry of zero amount).
func TestCompareMismatchOnCount(t *testing.T) {
	ledgerAgg := map[string]storage.TxAggregate{"tx-1": {Count: 2}}
	factAgg := map[string]storage.TxAggregate{"tx-1": {Count: 1}}
	discrepancies := Compare(ledgerAgg, factAgg)
	require.Len(t, discrepancies, 1)
	assert.Equal(t, KindAmountMismatch, discrepancies[0].Kind)
}
