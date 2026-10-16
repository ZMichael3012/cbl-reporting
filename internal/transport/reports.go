package transport

import (
	"context"
	"errors"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	reportingv1 "github.com/cbl/cbl-protos/gen/go/billing/reporting/v1"

	"github.com/cbl/cbl-reporting/internal/reports"
)

// ReportsServer реализует billing.reporting.v1.ReportingService поверх
// сервиса отчетов.
type ReportsServer struct {
	reportingv1.UnimplementedReportingServiceServer
	service *reports.Service
	log     *zap.Logger
}

// NewReportsServer собирает gRPC-сервер отчетов; nil-логгер заменяется пустым.
func NewReportsServer(service *reports.Service, log *zap.Logger) *ReportsServer {
	if log == nil {
		log = zap.NewNop()
	}
	return &ReportsServer{service: service, log: log}
}

// Query возвращает страницу отчета по каталогу; доменные ошибки переводятся в
// коды gRPC (NotFound/InvalidArgument), внутренние детали не раскрываются.
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

// mapQueryError переводит доменные ошибки в коды gRPC; внутренние ошибки
// логируются без деталей клиенту.
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
