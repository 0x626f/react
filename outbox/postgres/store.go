// Package postgres implements durable outbox storage on PostgreSQL using pgx.
// The caller owns the pool and every transaction passed to Store.Bind; the
// adapter never closes the pool or commits or rolls back the caller's outer
// transaction.
package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"sync/atomic"
	"time"

	"github.com/0x626f/react/outbox"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// IDB is the narrow pgx pool surface owned by the application.
type IDB interface {
	Begin(ctx context.Context) (pgx.Tx, error)
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// Config selects namespace, validated identifiers, limits, and generators.
type Config struct {
	Namespace          string
	Schema             string
	Table              string
	DuplicateMode      outbox.DuplicateMode
	DefaultMaxAttempts int
	MaxLeaseDuration   time.Duration
	Limits             outbox.Limits
	IDGenerator        outbox.IIDGenerator
	TokenGenerator     outbox.ITokenGenerator
}

// DefaultConfig returns adapter defaults for one shared table.
func DefaultConfig() Config {
	return Config{
		Namespace: "default", Schema: "react_outbox", Table: "records",
		DuplicateMode: outbox.RejectDuplicate, DefaultMaxAttempts: 10,
		MaxLeaseDuration: 5 * time.Minute, Limits: outbox.DefaultLimits(),
		IDGenerator: outbox.CryptoIDGenerator(), TokenGenerator: outbox.CryptoTokenGenerator(),
	}
}

var sqlIdentifier = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,39}$`)
var namespacePattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`)

func (config Config) normalized() (Config, error) {
	defaults := DefaultConfig()
	if config.Namespace == "" {
		config.Namespace = defaults.Namespace
	}
	if !namespacePattern.MatchString(config.Namespace) {
		return Config{}, fmt.Errorf("%w: invalid PostgreSQL namespace", outbox.ErrInvalidArgument)
	}
	if config.Schema == "" {
		config.Schema = defaults.Schema
	}
	if config.Table == "" {
		config.Table = defaults.Table
	}
	if !sqlIdentifier.MatchString(config.Schema) || !sqlIdentifier.MatchString(config.Table) {
		return Config{}, fmt.Errorf("%w: invalid PostgreSQL schema or table identifier", outbox.ErrInvalidArgument)
	}
	if !config.DuplicateMode.Valid() {
		return Config{}, fmt.Errorf("%w: duplicate mode", outbox.ErrInvalidArgument)
	}
	config.Limits = config.Limits.Normalized()
	if config.DefaultMaxAttempts == 0 {
		config.DefaultMaxAttempts = defaults.DefaultMaxAttempts
	}
	if config.DefaultMaxAttempts < 1 || config.DefaultMaxAttempts > config.Limits.MaxAttempts {
		return Config{}, fmt.Errorf("%w: default max attempts", outbox.ErrInvalidArgument)
	}
	if config.MaxLeaseDuration == 0 {
		config.MaxLeaseDuration = defaults.MaxLeaseDuration
	}
	if config.MaxLeaseDuration < time.Microsecond {
		return Config{}, fmt.Errorf("%w: max lease duration", outbox.ErrInvalidArgument)
	}
	if config.IDGenerator == nil {
		config.IDGenerator = defaults.IDGenerator
	}
	if config.TokenGenerator == nil {
		config.TokenGenerator = defaults.TokenGenerator
	}
	return config, nil
}

// Store implements the portable contracts with an application-owned pgx pool.
type Store struct {
	db     IDB
	config Config
	table  string
	closed atomic.Bool
}

// NewStore validates and constructs a PostgreSQL adapter without running migrations.
func NewStore(db IDB, config Config) (*Store, error) {
	if db == nil {
		return nil, fmt.Errorf("%w: PostgreSQL database is required", outbox.ErrInvalidArgument)
	}
	config, err := config.normalized()
	if err != nil {
		return nil, err
	}
	return &Store{db: db, config: config, table: pgx.Identifier{config.Schema, config.Table}.Sanitize()}, nil
}

// New is an alias for NewStore.
func New(db IDB, config Config) (*Store, error) { return NewStore(db, config) }

// Close only closes this adapter facade. The application-owned PostgreSQL pool
// remains open.
func (store *Store) Close() error { store.closed.Store(true); return nil }

func (store *Store) ensure(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if store.closed.Load() {
		return outbox.ErrClosed
	}
	return nil
}

const recordColumnList = `
id, destination, message_type, aggregate_type, aggregate_id, ordering_key,
idempotency_key, headers, payload, content_digest, state, attempts,
max_attempts, available_at, lease_owner, lease_token, lease_until,
last_error_code, last_error_message, created_at, updated_at, delivered_at,
dead_at, cancelled_at, version`

// IRowScanner is the shared row and rows scanning surface.
type IRowScanner interface{ Scan(dest ...any) error }

func scanRecord(row IRowScanner) (outbox.Record, error) {
	var record outbox.Record
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
		return outbox.Record{}, err
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
			return outbox.Record{}, fmt.Errorf("decode outbox headers: %w", err)
		}
	}
	record.AvailableAt = outbox.CanonicalTime(record.AvailableAt)
	record.CreatedAt = outbox.CanonicalTime(record.CreatedAt)
	record.UpdatedAt = outbox.CanonicalTime(record.UpdatedAt)
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
	canonical := outbox.CanonicalTime(*value)
	return &canonical
}

func nullable(value string) any {
	if value == "" {
		return nil
	}
	return value
}

func postgresStates(states []outbox.State) []string {
	values := make([]string, len(states))
	for index, state := range states {
		values[index] = string(state)
	}
	return values
}

func diagnoseState(ctx context.Context, queryer interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}, table, namespace string, id outbox.ID) (outbox.State, error) {
	var state outbox.State
	err := queryer.QueryRow(ctx, `SELECT state FROM `+table+` WHERE namespace=$1 AND id=$2`, namespace, id).Scan(&state)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", outbox.ErrNotFound
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

var _ outbox.IStore = (*Store)(nil)
