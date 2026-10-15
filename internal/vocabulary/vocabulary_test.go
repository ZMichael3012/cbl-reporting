package vocabulary

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRotateBudget проверяет бюджет синхронизации с ротацией: неполный набор
// проходит целиком, большие наборы обходятся по кругу без голодания хвоста.
func TestRotateBudget(t *testing.T) {
	chosen, cursor := rotateBudget([]string{"a", "b"}, "", 10)
	assert.Equal(t, []string{"a", "b"}, chosen)
	assert.Empty(t, cursor, "неполный набор сбрасывает курсор")

	all := []string{"a", "b", "c", "d", "e"}
	seen := map[string]int{}
	cursor = ""
	for cycle := 0; cycle < 3; cycle++ {
		chosen, cursor = rotateBudget(all, cursor, 2)
		require.Len(t, chosen, 2)
		for _, item := range chosen {
			seen[item]++
		}
	}
	assert.Len(t, seen, len(all), "за три цикла бюджет покрывает все элементы")
}

// TestDimRow проверяет валидацию строк Ledger перед записью в словарь.
func TestDimRow(t *testing.T) {
	cases := []struct {
		name        string
		accountID   string
		accountType string
		merchantRef string
		asset       string
		decimals    uint32
		want        bool
	}{
		{"valid merchant", "0f2f1e2d-1111-2222-3333-444455556666", "merchant", "m-1", "USDT", 6, true},
		{"valid payout", "acc-payout", "payout", "m-1", "USDT", 6, true},
		{"empty id", "", "merchant", "m-1", "USDT", 6, false},
		{"bad id", "acc'; DROP", "merchant", "m-1", "USDT", 6, false},
		{"long id", "0123456789012345678901234567890123456789012345678901234567890123456789", "merchant", "m-1", "USDT", 6, false},
		{"platform type", "acc-clearing", "clearing", "m-1", "USDT", 6, false},
		{"bad asset", "acc-1", "merchant", "m-1", "usdt", 6, false},
		{"overflow decimals", "acc-1", "merchant", "m-1", "USDT", 19, false},
		{"bad ref", "acc-1", "merchant", "m:1", "USDT", 6, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, ok := dimRow(tc.accountID, tc.accountType, tc.merchantRef, tc.asset, tc.decimals)
			assert.Equal(t, tc.want, ok)
		})
	}
}
