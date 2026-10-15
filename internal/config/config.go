// Пакет config собирает и валидирует конфигурацию сервиса reporting из
// переменных окружения (fail-fast на старте).
package config

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// hostPortPattern задает допустимую форму адреса gRPC-зависимости.
var hostPortPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9.-]*:[0-9]{1,5}$`)

// Config описывает проверенную конфигурацию сервиса.
type Config struct {
	// ClickHouseDSN задает строку подключения к ClickHouse (обязательна).
	ClickHouseDSN string
	// KafkaBrokers задает внутренний список брокеров для Kafka engine.
	KafkaBrokers string
	// SchemaPath задает путь wire-схемы относительно format_schema_path.
	SchemaPath string
	// MigrationsDir задает каталог goose-миграций.
	MigrationsDir string
	// HTTPAddr задает адрес административного HTTP-сервера.
	HTTPAddr string
	// GRPCAddr задает адрес gRPC-сервера (health).
	GRPCAddr string
	// MetricsAddr задает адрес сервера метрик.
	MetricsAddr string
	// LedgerGRPCAddr задает адрес LedgerService (обязателен).
	LedgerGRPCAddr string
	// LedgerTimeout задает таймаут вызовов Ledger.
	LedgerTimeout time.Duration
	// DimRefreshInterval задает интервал наполнения словаря.
	DimRefreshInterval time.Duration
	// DimMerchantsLimit задает предел мерчантов на цикл синхронизации.
	DimMerchantsLimit int
	// BackfillLimit задает предел счетов на цикл backfill.
	BackfillLimit int
	// CheckInterval задает интервал сверки с Ledger.
	CheckInterval time.Duration
	// CheckWindow задает размер окна сверки.
	CheckWindow time.Duration
	// CheckAccounts задает число счетов на цикл сверки.
	CheckAccounts int
	// CheckPageLimit задает размер страницы ListEntries.
	CheckPageLimit uint32
	// CheckMaxPages задает предел страниц на счет за цикл.
	CheckMaxPages int
}

// Load собирает конфигурацию через переданный доступ к переменным окружения
// (nil значение - значение по умолчанию) и валидирует все ограничения.
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
	// Однократный проход окна: при интервале больше окна между циклами
	// остаются непроверенные отрезки таймлайна.
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
	cfg.CheckPageLimit = uint32(pageLimit) //nolint:gosec // границы intEnv: 1 - 10000, переполнение невозможно
	if cfg.CheckMaxPages, err = intEnv(getenv, "LEDGER_CHECK_MAX_PAGES", 10, 1, 100); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

// validHostPort проверяет форму host:port с допустимым диапазоном порта.
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

// fallback возвращает значение или запасной вариант для пустой строки.
func fallback(value, defaultValue string) string {
	if value == "" {
		return defaultValue
	}
	return value
}

// durationEnv разбирает длительность из окружения с границами включительно.
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

// intEnv разбирает целое из окружения с границами включительно.
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
