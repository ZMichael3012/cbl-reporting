package transport

import (
	"context"
	"errors"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	reportingv1 "github.com/ZMichael3012/cbl-protos/gen/go/billing/reporting/v1"

	"github.com/cbl/cbl-reporting/internal/reports"
)

// ReportsServer implements billing.reporting.v1.ReportingService over the
// reports service.
type ReportsServer struct {
	reportingv1.UnimplementedReportingServiceServer
	service *reports.Service
	log     *zap.Logger
}

// NewReportsServer assembles the reports gRPC server; a nil logger is replaced by an empty one.
func NewReportsServer(service *reports.Service, log *zap.Logger) *ReportsServer {
	if log == nil {
		log = zap.NewNop()
	}
	return &ReportsServer{service: service, log: log}
}

// Query returns a report page from the catalog; domain errors are mapped to
// gRPC codes (NotFound/InvalidArgument), internal details are not disclosed.
func (s *ReportsServer) Query(ctx context.Context, request *reportingv1.QueryRequest) (*reportingv1.QueryResponse, error) {
	query := reports.QueryRequest{
		ReportType: request.GetReportType(),
		MerchantID: request.GetMerchantId(),
		GroupBy:    request.GetGroupBy(),
		Limit:      request.GetLimit(),
		Cursor:     request.GetCursor(),
	}
	if from := request.GetPeriodFrom(); from != nil {
		value := from.AsTime()
		query.PeriodFrom = &value
	}
	if to := request.GetPeriodTo(); to != nil {
		value := to.AsTime()
		query.PeriodTo = &value
	}
	page, err := s.service.Query(ctx, query)
	if err != nil {
		return nil, s.mapQueryError(err, request.GetReportType())
	}
	rows := make([]*reportingv1.ReportRow, 0, len(page.Rows))
	for _, row := range page.Rows {
		rows = append(rows, &reportingv1.ReportRow{Values: row})
	}
	return &reportingv1.QueryResponse{Rows: rows, NextCursor: page.NextCursor}, nil
}

// mapQueryError maps domain errors to gRPC codes; internal errors are
// logged without disclosing details to the client.
func (s *ReportsServer) mapQueryError(err error, reportType string) error {
	switch {
	case errors.Is(err, reports.ErrUnknownReport):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, reports.ErrInvalidArgument):
		return status.Error(codes.InvalidArgument, err.Error())
	default:
		s.log.Error("report query failed", zap.String("report_type", reportType), zap.Error(err))
		return status.Error(codes.Internal, "report query failed")
	}
}
