package config

import (
	"strings"
	"testing"
	"time"
)

// envMap returns an environment accessor function backed by a map.
func envMap(values map[string]string) func(string) string {
	return func(key string) string { return values[key] }
}

// baseEnv defines the minimally valid environment.
func baseEnv() map[string]string {
	return map[string]string{ //nolint:gosec // local stand fixture, not a secret
		"CLICKHOUSE_DSN":   "clickhouse://cbl:cbl@clickhouse:9000/reporting",
		"KAFKA_BROKERS":    "kafka:29092",
		"LEDGER_GRPC_ADDR": "ledger:9040",
	}
}

// TestLoadDefaults verifies the default values and compatibility with 11.2.
func TestLoadDefaults(t *testing.T) {
	cfg, err := Load(envMap(baseEnv()))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.SchemaPath != "events.proto" || cfg.MigrationsDir != "./migrations" {
		t.Errorf("defaults 11.2 changed: %+v", cfg)
	}
	if cfg.HTTPAddr != ":8110" || cfg.GRPCAddr != ":9110" || cfg.MetricsAddr != ":9310" {
		t.Errorf("defaults 11.2 changed: %+v", cfg)
	}
	if cfg.LedgerTimeout != 5*time.Second || cfg.DimRefreshInterval != 5*time.Minute {
		t.Errorf("unexpected sync defaults: %+v", cfg)
	}
	if cfg.BackfillLimit != 1000 || cfg.CheckAccounts != 10 || cfg.CheckPageLimit != 1000 || cfg.CheckMaxPages != 10 {
		t.Errorf("unexpected limits: %+v", cfg)
	}
	if cfg.CheckInterval != 5*time.Minute || cfg.CheckWindow != 5*time.Minute {
		t.Errorf("unexpected check defaults: %+v", cfg)
	}
}

// TestLoadRequired verifies the required variables.
func TestLoadRequired(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(map[string]string)
	}{
		{"dsn", func(env map[string]string) { delete(env, "CLICKHOUSE_DSN") }},
		{"brokers", func(env map[string]string) { delete(env, "KAFKA_BROKERS") }},
		{"ledger", func(env map[string]string) { delete(env, "LEDGER_GRPC_ADDR") }},
		{"ledger form", func(env map[string]string) { env["LEDGER_GRPC_ADDR"] = "ledger" }},
		{"ledger port", func(env map[string]string) { env["LEDGER_GRPC_ADDR"] = "ledger:70000" }},
		{"ledger injection", func(env map[string]string) { env["LEDGER_GRPC_ADDR"] = "ledger:9040'; DROP" }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			env := baseEnv()
			tc.mutate(env)
			if _, err := Load(envMap(env)); err == nil {
				t.Fatal("Load succeeded, want error")
			}
		})
	}
}

// TestLoadValues verifies parsing and value bounds.
func TestLoadValues(t *testing.T) {
	env := baseEnv()
	env["LEDGER_TIMEOUT"] = "2s"
	env["DIM_REFRESH_INTERVAL"] = "30s"
	env["BACKFILL_LIMIT"] = "50"
	env["LEDGER_CHECK_INTERVAL"] = "10s"
	env["LEDGER_CHECK_WINDOW"] = "15s"
	env["LEDGER_CHECK_ACCOUNTS"] = "3"
	env["DIM_MERCHANTS_LIMIT"] = "25"
	env["LEDGER_CHECK_PAGE_LIMIT"] = "100"
	env["LEDGER_CHECK_MAX_PAGES"] = "2"
	cfg, err := Load(envMap(env))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.LedgerTimeout != 2*time.Second || cfg.DimRefreshInterval != 30*time.Second {
		t.Errorf("parsed values mismatch: %+v", cfg)
	}
	if cfg.BackfillLimit != 50 || cfg.DimMerchantsLimit != 25 {
		t.Errorf("parsed values mismatch: %+v", cfg)
	}
	if cfg.CheckInterval != 10*time.Second || cfg.CheckWindow != 15*time.Second || cfg.CheckAccounts != 3 {
		t.Errorf("parsed check values mismatch: %+v", cfg)
	}
	if cfg.CheckPageLimit != 100 || cfg.CheckMaxPages != 2 {
		t.Errorf("parsed page limits mismatch: %+v", cfg)
	}
}

// TestLoadInvalid verifies rejection of invalid values.
func TestLoadInvalid(t *testing.T) {
	cases := []struct {
		key   string
		value string
	}{
		{"LEDGER_TIMEOUT", "0s"},
		{"LEDGER_TIMEOUT", "5m"},
		{"LEDGER_TIMEOUT", "fast"},
		{"DIM_REFRESH_INTERVAL", "500ms"},
		{"BACKFILL_LIMIT", "0"},
		{"BACKFILL_LIMIT", "200000"},
		{"BACKFILL_LIMIT", "many"},
		{"LEDGER_CHECK_INTERVAL", "0s"},
		{"LEDGER_CHECK_WINDOW", "25h"},
		{"LEDGER_CHECK_WINDOW", "1s"},
		{"DIM_MERCHANTS_LIMIT", "0"},
		{"DIM_MERCHANTS_LIMIT", "many"},
		{"LEDGER_CHECK_ACCOUNTS", "0"},
		{"LEDGER_CHECK_PAGE_LIMIT", "0"},
		{"LEDGER_CHECK_PAGE_LIMIT", "10001"},
		{"LEDGER_CHECK_MAX_PAGES", "101"},
	}
	for _, tc := range cases {
		t.Run(tc.key+"="+tc.value, func(t *testing.T) {
			env := baseEnv()
			env[tc.key] = tc.value
			_, err := Load(envMap(env))
			if err == nil {
				t.Fatal("Load succeeded, want error")
			}
			if !strings.Contains(err.Error(), tc.key) {
				t.Errorf("error %q does not mention %s", err, tc.key)
			}
		})
	}
}
