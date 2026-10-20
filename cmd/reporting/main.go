// main starts the CBL platform reporting service: ClickHouse migrations,
// Kafka pipeline reconciliation, the dim_accounts dictionary, reconciliation
// with Ledger and administrative endpoints.
package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"github.com/ZMichael3012/cbl-kit/ginmw"
	"github.com/ZMichael3012/cbl-kit/grpcmw"

	reportingv1 "github.com/ZMichael3012/cbl-protos/gen/go/billing/reporting/v1"

	"github.com/cbl/cbl-reporting/internal/config"
	"github.com/cbl/cbl-reporting/internal/ledger"
	"github.com/cbl/cbl-reporting/internal/ledgercheck"
	"github.com/cbl/cbl-reporting/internal/metrics"
	"github.com/cbl/cbl-reporting/internal/reports"
	"github.com/cbl/cbl-reporting/internal/storage"
	"github.com/cbl/cbl-reporting/internal/transport"
	"github.com/cbl/cbl-reporting/internal/vocabulary"
)

// main starts the service and exits the process with a non-zero code on error.
func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "fatal:", err)
		os.Exit(1)
	}
}

// run assembles the pipeline, dictionary and reconciliation dependencies and
// serves requests until a stop signal.
func run() error {
	log, err := zap.NewProduction()
	if err != nil {
		return fmt.Errorf("logger: %w", err)
	}
	defer func() { _ = log.Sync() }()

	cfg, err := config.Load(os.Getenv)
	if err != nil {
		return fmt.Errorf("config: %w", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	db, err := storage.Open(cfg.ClickHouseDSN)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()

	if err := storage.Migrate(ctx, db, cfg.MigrationsDir); err != nil {
		return fmt.Errorf("apply migrations: %w", err)
	}
	pipeline, err := storage.NewPipeline(db, cfg.KafkaBrokers, cfg.SchemaPath, log)
	if err != nil {
		return fmt.Errorf("pipeline configuration: %w", err)
	}
	if err := pipeline.Reconcile(ctx); err != nil {
		return fmt.Errorf("reconcile pipeline: %w", err)
	}
	log.Info("ingestion pipeline reconciled",
		zap.String("brokers", cfg.KafkaBrokers),
		zap.String("schema", cfg.SchemaPath))

	ledgerClient, err := ledger.NewClient(cfg.LedgerGRPCAddr, cfg.LedgerTimeout)
	if err != nil {
		return fmt.Errorf("ledger client: %w", err)
	}
	defer func() { _ = ledgerClient.Close() }()

	reportsService, err := reports.New(db)
	if err != nil {
		return fmt.Errorf("reports service: %w", err)
	}

	counters := &metrics.Counters{}
	syncer, err := vocabulary.New(db, ledgerClient, cfg.DimRefreshInterval, cfg.BackfillLimit, cfg.DimMerchantsLimit, log, counters)
	if err != nil {
		return fmt.Errorf("vocabulary worker: %w", err)
	}
	checker, err := ledgercheck.New(db, ledgerClient, cfg.CheckInterval, cfg.CheckWindow,
		cfg.CheckAccounts, cfg.CheckPageLimit, cfg.CheckMaxPages, log, counters)
	if err != nil {
		return fmt.Errorf("ledger check worker: %w", err)
	}

	var workers sync.WaitGroup
	workers.Add(2)
	go func() {
		defer workers.Done()
		syncer.Run(ctx)
	}()
	go func() {
		defer workers.Done()
		checker.Run(ctx)
	}()

	healthSrv := health.NewServer()
	healthSrv.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)

	router := gin.New()
	router.Use(ginmw.RequestID(), ginmw.AccessLog(log), ginmw.Recovery(log))
	router.GET("/healthz", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	})
	router.GET("/readyz", func(c *gin.Context) {
		readyCtx, cancel := context.WithTimeout(c.Request.Context(), 3*time.Second)
		defer cancel()
		if readyErr := storage.Ready(readyCtx, db); readyErr != nil {
			healthSrv.SetServingStatus("", healthpb.HealthCheckResponse_NOT_SERVING)
			c.JSON(http.StatusServiceUnavailable, gin.H{"status": "unavailable"})
			return
		}
		healthSrv.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	})

	httpSrv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           router,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	metricsSrv := &http.Server{Addr: cfg.MetricsAddr, Handler: counters.Handler(), ReadHeaderTimeout: 5 * time.Second}
	// kit interceptor: trace/request-id and panic recovery of Query.
	grpcSrv := grpc.NewServer(grpc.UnaryInterceptor(grpcmw.UnaryServer(log)))
	healthpb.RegisterHealthServer(grpcSrv, healthSrv)
	reportingv1.RegisterReportingServiceServer(grpcSrv, transport.NewReportsServer(reportsService, log))

	serveErrors := make(chan error, 3)
	go func() {
		log.Info("http server started", zap.String("addr", cfg.HTTPAddr))
		if serveErr := httpSrv.ListenAndServe(); serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			log.Error("http server failed", zap.Error(serveErr))
			serveErrors <- fmt.Errorf("http server: %w", serveErr)
			stop()
		}
	}()
	go func() {
		log.Info("metrics server started", zap.String("addr", cfg.MetricsAddr))
		if serveErr := metricsSrv.ListenAndServe(); serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			log.Error("metrics server failed", zap.Error(serveErr))
			serveErrors <- fmt.Errorf("metrics server: %w", serveErr)
			stop()
		}
	}()
	go func() {
		listener, listenErr := net.Listen("tcp", cfg.GRPCAddr)
		if listenErr != nil {
			log.Error("grpc listener failed", zap.Error(listenErr))
			serveErrors <- fmt.Errorf("grpc listener: %w", listenErr)
			stop()
			return
		}
		log.Info("grpc server started", zap.String("addr", cfg.GRPCAddr))
		if serveErr := grpcSrv.Serve(listener); serveErr != nil {
			log.Error("grpc server failed", zap.Error(serveErr))
			serveErrors <- fmt.Errorf("grpc server: %w", serveErr)
			stop()
		}
	}()

	var serveFailure error
	select {
	case <-ctx.Done():
	case serveFailure = <-serveErrors:
	}
	if serveFailure == nil {
		select {
		case serveFailure = <-serveErrors:
		default:
		}
	}
	log.Info("shutdown started")
	// Workers stop before servers: cycles do not start new work on a
	// closing ClickHouse connection.
	stop()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if shutdownErr := httpSrv.Shutdown(shutdownCtx); shutdownErr != nil {
		log.Error("http shutdown failed", zap.Error(shutdownErr))
	}
	if shutdownErr := metricsSrv.Shutdown(shutdownCtx); shutdownErr != nil {
		log.Error("metrics shutdown failed", zap.Error(shutdownErr))
	}
	grpcSrv.GracefulStop()
	workersDone := make(chan struct{})
	go func() {
		workers.Wait()
		close(workersDone)
	}()
	select {
	case <-workersDone:
	case <-time.After(10 * time.Second):
		log.Warn("workers did not finish in time")
	}
	log.Info("shutdown finished")
	return serveFailure
}
