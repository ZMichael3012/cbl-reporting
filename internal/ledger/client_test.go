package ledger

import (
	"testing"

	"github.com/stretchr/testify/require"

	commonv1 "github.com/cbl/cbl-protos/gen/go/billing/common/v1"
)

// TestDirectionName проверяет маппинг направлений контракта в имена колонок
// фактов и отказ на неизвестных значениях.
func TestDirectionName(t *testing.T) {
	debit, err := directionName(commonv1.Direction_DIRECTION_DEBIT)
	require.NoError(t, err)
	require.Equal(t, "debit", debit)

	credit, err := directionName(commonv1.Direction_DIRECTION_CREDIT)
	require.NoError(t, err)
	require.Equal(t, "credit", credit)

	_, err = directionName(commonv1.Direction_DIRECTION_UNSPECIFIED)
	require.Error(t, err)
}
