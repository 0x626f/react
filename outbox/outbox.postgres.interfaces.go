package outbox

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// IPostgresDB is the narrow pgx pool surface owned by the application.
type IPostgresDB interface {
	Begin(ctx context.Context) (pgx.Tx, error)
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// IPostgresRowScanner is the shared row and rows scanning surface.
type IPostgresRowScanner interface{ Scan(dest ...any) error }
