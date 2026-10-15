package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
)

// Open открывает подключение ClickHouse по DSN сервиса. Ошибка разбора DSN не
// эхоит исходную строку: в ней может быть пароль (утечка в логи при опечатке
// конфигурации).
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

// Ping проверяет доступность ClickHouse (readyz).
func Ping(ctx context.Context, db *sql.DB) error {
	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("clickhouse ping: %w", err)
	}
	return nil
}

// Ready проверяет готовность хранилища и конвейера: ClickHouse отвечает и все
// четыре пары Kafka-таблица+materialized view на месте.
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
