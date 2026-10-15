package storage

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestBackfillAccountValidation проверяет отказ на строках словаря вне плана
// счетов до обращения к ClickHouse (db не используется на пути валидации).
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
