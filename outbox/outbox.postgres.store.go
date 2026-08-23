// PostgresStore implements durable outbox storage using pgx.
// The caller owns the pool and every transaction passed to PostgresStore.Bind; the
// adapter never closes the pool or commits or rolls back the caller's outer
// transaction.
package outbox

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
)

// PostgresStore implements the portable contracts with an application-owned pgx pool.
type PostgresStore struct {
	db     IPostgresDB
	config PostgresConfig
	table  string
	closed atomic.Bool
}

// NewPostgresStore validates and constructs a PostgreSQL adapter without running migrations.
func NewPostgresStore(db IPostgresDB, config PostgresConfig) (*PostgresStore, error) {
	if db == nil {
		return nil, fmt.Errorf("%w: PostgreSQL database is required", ErrInvalidArgument)
	}
	config, err := config.normalized()
	if err != nil {
		return nil, err
	}
	return &PostgresStore{db: db, config: config, table: pgx.Identifier{config.Schema, config.Table}.Sanitize()}, nil
}

// Close only closes this adapter facade. The application-owned PostgreSQL pool
// remains open.
func (store *PostgresStore) Close() error { store.closed.Store(true); return nil }

func (store *PostgresStore) ensure(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if store.closed.Load() {
		return ErrClosed
	}
	return nil
}

const recordColumnList = `
id, destination, message_type, aggregate_type, aggregate_id, ordering_key,
idempotency_key, headers, payload, content_digest, state, attempts,
max_attempts, available_at, lease_owner, lease_token, lease_until,
last_error_code, last_error_message, created_at, updated_at, delivered_at,
dead_at, cancelled_at, version`

func scanRecord(row IPostgresRowScanner) (Record, error) {
	var record Record
	var headers []byte
	var aggregateType, aggregateID, orderingKey, idempotencyKey *string
	var leaseOwner, leaseToken, lastErrorCode, lastErrorMessage *string
	err := row.Scan(
		&record.ID, &record.Destination, &record.MessageType, &aggregateType,
		&aggregateID, &orderingKey, &idempotencyKey, &headers, &record.Payload,
		&record.ContentDigest, &record.State, &record.Attempts, &record.MaxAttempts,
		&record.AvailableAt, &leaseOwner, &leaseToken, &record.LeaseUntil,
		&lastErrorCode, &lastErrorMessage, &record.CreatedAt, &record.UpdatedAt,
		&record.DeliveredAt, &record.DeadAt, &record.CancelledAt, &record.Version,
	)
	if err != nil {
		return Record{}, err
	}
	if aggregateType != nil {
		record.AggregateType = *aggregateType
	}
	if aggregateID != nil {
		record.AggregateID = *aggregateID
	}
	if orderingKey != nil {
		record.OrderingKey = *orderingKey
	}
	if idempotencyKey != nil {
		record.IdempotencyKey = *idempotencyKey
	}
	if leaseOwner != nil {
		record.LeaseOwner = *leaseOwner
	}
	if leaseToken != nil {
		record.LeaseToken = *leaseToken
	}
	if lastErrorCode != nil {
		record.LastErrorCode = *lastErrorCode
	}
	if lastErrorMessage != nil {
		record.LastErrorMessage = *lastErrorMessage
	}
	if len(headers) > 0 {
		if err = json.Unmarshal(headers, &record.Headers); err != nil {
			return Record{}, fmt.Errorf("decode outbox headers: %w", err)
		}
	}
	record.AvailableAt = CanonicalTime(record.AvailableAt)
	record.CreatedAt = CanonicalTime(record.CreatedAt)
	record.UpdatedAt = CanonicalTime(record.UpdatedAt)
	record.LeaseUntil = canonicalPointer(record.LeaseUntil)
	record.DeliveredAt = canonicalPointer(record.DeliveredAt)
	record.DeadAt = canonicalPointer(record.DeadAt)
	record.CancelledAt = canonicalPointer(record.CancelledAt)
	return record.Clone(), nil
}

func canonicalPointer(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	canonical := CanonicalTime(*value)
	return &canonical
}

func nullable(value string) any {
	if value == "" {
		return nil
	}
	return value
}

func postgresStates(states []State) []string {
	values := make([]string, len(states))
	for index, state := range states {
		values[index] = string(state)
	}
	return values
}

func diagnoseState(ctx context.Context, queryer interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}, table, namespace string, id ID) (State, error) {
	var state State
	err := queryer.QueryRow(ctx, `SELECT state FROM `+table+` WHERE namespace=$1 AND id=$2`, namespace, id).Scan(&state)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", ErrNotFound
	}
	return state, err
}

func placeholderList(start, count int) string {
	values := make([]string, count)
	for index := range count {
		values[index] = fmt.Sprintf("$%d", start+index)
	}
	return strings.Join(values, ",")
}

var _ IStore = (*PostgresStore)(nil)
