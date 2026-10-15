package storage

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/pressly/goose/v3"
)

// Migrate применяет миграции сервиса (goose, диалект clickhouse) только вперед.
func Migrate(ctx context.Context, db *sql.DB, dir string) error {
	if err := goose.SetDialect("clickhouse"); err != nil {
		return fmt.Errorf("goose dialect: %w", err)
	}
	if err := goose.UpContext(ctx, db, dir); err != nil {
		return fmt.Errorf("goose up: %w", err)
	}
	return nil
}
