package transport

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/cbl/cbl-reporting/internal/reports"
)

// TestMapQueryError checks the mapping of domain errors to gRPC codes;
// internal errors do not disclose details to the client.
func TestMapQueryError(t *testing.T) {
	server := NewReportsServer(nil, nil)

	err := server.mapQueryError(errors.Join(reports.ErrUnknownReport, errors.New("invoices")), "invoices")
	require.Equal(t, codes.NotFound, status.Code(err))

	err = server.mapQueryError(errors.Join(reports.ErrInvalidArgument, errors.New("limit 101")), "payments")
	require.Equal(t, codes.InvalidArgument, status.Code(err))

	err = server.mapQueryError(errors.New("clickhouse connection refused"), "payments")
	require.Equal(t, codes.Internal, status.Code(err))
	require.NotContains(t, err.Error(), "connection refused", "internal error details are not returned")
}
