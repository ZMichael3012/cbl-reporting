package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
)

// Open opens a ClickHouse connection by the service DSN. A DSN parse error
// does not echo the original string: it may contain a password (a leak to
// logs on a configuration typo).
func Open(dsn string) (*sql.DB, error) {
	options, err := clickhouse.ParseDSN(dsn)
	if err != nil {
		return nil, errors.New("parse clickhouse dsn: invalid dsn")
	}
	db := clickhouse.OpenDB(options)
	db.SetMaxOpenConns(5)
	db.SetMaxIdleConns(2)
	db.SetConnMaxLifetime(30 * time.Minute)
	return db, nil
}

// Ping checks ClickHouse availability (readyz).
func Ping(ctx context.Context, db *sql.DB) error {
	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("clickhouse ping: %w", err)
	}
	return nil
}

// Ready checks the readiness of the storage and the pipeline: ClickHouse
// responds and all four Kafka table + materialized view pairs are in place.
func Ready(ctx context.Context, db *sql.DB) error {
	if err := Ping(ctx, db); err != nil {
		return err
	}
	var tables uint64
	if err := db.QueryRowContext(ctx,
		"SELECT count() FROM system.tables WHERE database = 'reporting' AND name LIKE 'kafka\\_%'").Scan(&tables); err != nil {
		return fmt.Errorf("read pipeline state: %w", err)
	}
	var views uint64
	if err := db.QueryRowContext(ctx,
		"SELECT count() FROM system.tables WHERE database = 'reporting' AND name LIKE '%\\_mv'").Scan(&views); err != nil {
		return fmt.Errorf("read pipeline views: %w", err)
	}
	if tables != 4 || views != 4 {
		return fmt.Errorf("pipeline is incomplete: kafka tables %d, views %d", tables, views)
	}
	return nil
}
