package storage

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestBackfillAccountValidation checks rejection of dictionary rows outside
// the chart of accounts before accessing ClickHouse (db is not used on the
// validation path).
func TestBackfillAccountValidation(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name    string
		account DimAccount
	}{
		{"unknown type", DimAccount{AccountID: "a", AccountType: "reserve"}},
		{"merchant without ref", DimAccount{AccountID: "a", AccountType: "merchant"}},
		{"merchant bad ref", DimAccount{AccountID: "a", AccountType: "merchant", MerchantID: "m:1"}},
		{"platform with ref", DimAccount{AccountID: "a", AccountType: "clearing", MerchantID: "m-1"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Error(t, BackfillAccount(ctx, nil, tc.account))
		})
	}
}
