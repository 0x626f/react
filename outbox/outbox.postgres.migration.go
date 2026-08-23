package outbox

import (
	"context"
	_ "embed"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
)

//go:embed outbox.postgres.migration.001.sql
var migration001 string

// PostgresMigration is one ordered adapter-owned schema change.
type PostgresMigration struct {
	Version int
	Name    string
	SQL     string
}

// Migrations returns rendered, safely quoted adapter migrations in version order.
func (store *PostgresStore) Migrations() []PostgresMigration {
	replacer := strings.NewReplacer(
		"{{schema}}", pgx.Identifier{store.config.Schema}.Sanitize(),
		"{{table}}", store.table,
		"{{claim_index}}", pgx.Identifier{store.config.Table + "_claim_idx"}.Sanitize(),
		"{{lease_index}}", pgx.Identifier{store.config.Table + "_lease_idx"}.Sanitize(),
		"{{destination_index}}", pgx.Identifier{store.config.Table + "_destination_idx"}.Sanitize(),
		"{{terminal_index}}", pgx.Identifier{store.config.Table + "_terminal_idx"}.Sanitize(),
		"{{idempotency_index}}", pgx.Identifier{store.config.Table + "_idempotency_idx"}.Sanitize(),
	)
	return []PostgresMigration{{Version: 1, Name: "create_records", SQL: replacer.Replace(migration001)}}
}

// Migrate applies the adapter-owned, versioned migrations. Applications may
// instead consume Migrations from their normal migration system.
func (store *PostgresStore) Migrate(ctx context.Context) error {
	if err := store.ensure(ctx); err != nil {
		return err
	}
	schema := pgx.Identifier{store.config.Schema}.Sanitize()
	migrationsTable := pgx.Identifier{store.config.Schema, store.config.Table + "_migrations"}.Sanitize()
	if _, err := store.db.Exec(ctx, `CREATE SCHEMA IF NOT EXISTS `+schema); err != nil {
		return err
	}
	if _, err := store.db.Exec(ctx, `CREATE TABLE IF NOT EXISTS `+migrationsTable+` (
		version integer PRIMARY KEY, name text NOT NULL, applied_at timestamptz NOT NULL DEFAULT clock_timestamp()
	)`); err != nil {
		return err
	}
	for _, migration := range store.Migrations() {
		tx, err := store.db.Begin(ctx)
		if err != nil {
			return err
		}
		func() {
			defer func() { _ = tx.Rollback(context.Background()) }()
			if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtext($1))`, fmt.Sprintf("react-outbox:%s:%s", store.config.Schema, store.config.Table)); err != nil {
				return
			}
			var exists bool
			if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM `+migrationsTable+` WHERE version=$1)`, migration.Version).Scan(&exists); err != nil || exists {
				if exists {
					err = nil
				}
				return
			}
			if _, err = tx.Exec(ctx, migration.SQL); err != nil {
				return
			}
			if _, err = tx.Exec(ctx, `INSERT INTO `+migrationsTable+` (version,name) VALUES ($1,$2)`, migration.Version, migration.Name); err != nil {
				return
			}
			err = tx.Commit(ctx)
		}()
		if err != nil {
			return err
		}
	}
	return nil
}
