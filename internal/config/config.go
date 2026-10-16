// Package config assembles and validates the configuration of the reporting
// service from environment variables (fail-fast at startup).
package config

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// hostPortPattern defines the allowed form of a gRPC dependency address.
var hostPortPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9.-]*:[0-9]{1,5}$`)

// Config describes the validated configuration of the service.
type Config struct {
	// ClickHouseDSN sets the ClickHouse connection string (required).
	ClickHouseDSN string
	// KafkaBrokers sets the internal broker list for the Kafka engine.
	KafkaBrokers string
	// SchemaPath sets the wire schema path relative to format_schema_path.
	SchemaPath string
	// MigrationsDir sets the directory of goose migrations.
	MigrationsDir string
	// HTTPAddr sets the address of the administrative HTTP server.
	HTTPAddr string
	// GRPCAddr sets the address of the gRPC server (health).
	GRPCAddr string
	// MetricsAddr sets the address of the metrics server.
	MetricsAddr string
	// LedgerGRPCAddr sets the LedgerService address (required).
	LedgerGRPCAddr string
	// LedgerTimeout sets the timeout of Ledger calls.
	LedgerTimeout time.Duration
	// DimRefreshInterval sets the dictionary refresh interval.
	DimRefreshInterval time.Duration
	// DimMerchantsLimit sets the merchant limit per synchronization cycle.
	DimMerchantsLimit int
	// BackfillLimit sets the account limit per backfill cycle.
	BackfillLimit int
	// CheckInterval sets the interval of reconciliation with Ledger.
	CheckInterval time.Duration
	// CheckWindow sets the size of the reconciliation window.
	CheckWindow time.Duration
	// CheckAccounts sets the number of accounts per reconciliation cycle.
	CheckAccounts int
	// CheckPageLimit sets the size of a ListEntries page.
	CheckPageLimit uint32
	// CheckMaxPages sets the page limit per account per cycle.
	CheckMaxPages int
}

// Load assembles the configuration through the given environment accessor
// (an empty value means the default) and validates all constraints.
func Load(getenv func(string) string) (Config, error) {
	cfg := Config{
		ClickHouseDSN:  getenv("CLICKHOUSE_DSN"),
		KafkaBrokers:   getenv("KAFKA_BROKERS"),
		SchemaPath:     fallback(getenv("SCHEMA_PATH"), "events.proto"),
		MigrationsDir:  fallback(getenv("MIGRATIONS_DIR"), "./migrations"),
		HTTPAddr:       fallback(getenv("HTTP_ADDR"), ":8110"),
		GRPCAddr:       fallback(getenv("GRPC_ADDR"), ":9110"),
		MetricsAddr:    fallback(getenv("METRICS_ADDR"), ":9310"),
		LedgerGRPCAddr: getenv("LEDGER_GRPC_ADDR"),
	}
	if cfg.ClickHouseDSN == "" {
		return Config{}, errors.New("CLICKHOUSE_DSN is required")
	}
	if cfg.KafkaBrokers == "" {
		return Config{}, errors.New("KAFKA_BROKERS is required")
	}
	if cfg.LedgerGRPCAddr == "" {
		return Config{}, errors.New("LEDGER_GRPC_ADDR is required")
	}
	if !validHostPort(cfg.LedgerGRPCAddr) {
		return Config{}, fmt.Errorf("LEDGER_GRPC_ADDR has unexpected form: %q", cfg.LedgerGRPCAddr)
	}
	var err error
	if cfg.LedgerTimeout, err = durationEnv(getenv, "LEDGER_TIMEOUT", 5*time.Second, time.Second, time.Minute); err != nil {
		return Config{}, err
	}
	if cfg.DimRefreshInterval, err = durationEnv(getenv, "DIM_REFRESH_INTERVAL", 5*time.Minute, time.Second, 24*time.Hour); err != nil {
		return Config{}, err
	}
	if cfg.DimMerchantsLimit, err = intEnv(getenv, "DIM_MERCHANTS_LIMIT", 200, 1, 10_000); err != nil {
		return Config{}, err
	}
	if cfg.BackfillLimit, err = intEnv(getenv, "BACKFILL_LIMIT", 1000, 1, 100_000); err != nil {
		return Config{}, err
	}
	if cfg.CheckInterval, err = durationEnv(getenv, "LEDGER_CHECK_INTERVAL", 5*time.Minute, time.Second, 24*time.Hour); err != nil {
		return Config{}, err
	}
	if cfg.CheckWindow, err = durationEnv(getenv, "LEDGER_CHECK_WINDOW", 5*time.Minute, time.Second, 24*time.Hour); err != nil {
		return Config{}, err
	}
	// A single pass over the window: with an interval greater than the window,
	// unchecked timeline segments remain between cycles.
	if cfg.CheckWindow < cfg.CheckInterval {
		return Config{}, fmt.Errorf("LEDGER_CHECK_WINDOW (%s) must be not less than LEDGER_CHECK_INTERVAL (%s)", cfg.CheckWindow, cfg.CheckInterval)
	}
	if cfg.CheckAccounts, err = intEnv(getenv, "LEDGER_CHECK_ACCOUNTS", 10, 1, 10_000); err != nil {
		return Config{}, err
	}
	pageLimit, err := intEnv(getenv, "LEDGER_CHECK_PAGE_LIMIT", 1000, 1, 10_000)
	if err != nil {
		return Config{}, err
	}
	cfg.CheckPageLimit = uint32(pageLimit) //nolint:gosec // intEnv bounds: 1 - 10000, overflow is impossible
	if cfg.CheckMaxPages, err = intEnv(getenv, "LEDGER_CHECK_MAX_PAGES", 10, 1, 100); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

// validHostPort checks the host:port form with an allowed port range.
func validHostPort(value string) bool {
	if !hostPortPattern.MatchString(value) {
		return false
	}
	port, err := strconv.Atoi(value[strings.LastIndex(value, ":")+1:])
	if err != nil || port < 1 || port > 65535 {
		return false
	}
	return true
}

// fallback returns the value or the fallback for an empty string.
func fallback(value, defaultValue string) string {
	if value == "" {
		return defaultValue
	}
	return value
}

// durationEnv parses a duration from the environment with inclusive bounds.
func durationEnv(getenv func(string) string, key string, defaultValue, minValue, maxValue time.Duration) (time.Duration, error) {
	raw := getenv(key)
	if raw == "" {
		return defaultValue, nil
	}
	value, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("%s has unexpected form: %q", key, raw)
	}
	if value < minValue || value > maxValue {
		return 0, fmt.Errorf("%s is out of range: %s", key, raw)
	}
	return value, nil
}

// intEnv parses an integer from the environment with inclusive bounds.
func intEnv(getenv func(string) string, key string, defaultValue, minValue, maxValue int) (int, error) {
	raw := getenv(key)
	if raw == "" {
		return defaultValue, nil
	}
	value, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("%s has unexpected form: %q", key, raw)
	}
	if value < minValue || value > maxValue {
		return 0, fmt.Errorf("%s is out of range: %s", key, raw)
	}
	return value, nil
}
