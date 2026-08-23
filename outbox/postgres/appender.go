package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/0x626f/react/outbox"
	"github.com/jackc/pgx/v5"
)

// TxAppender appends through a caller-owned pgx transaction.
type TxAppender struct {
	store *Store
	tx    pgx.Tx
}

// Bind creates an appender on the caller's pgx transaction. Append uses a
// nested pgx transaction (a savepoint) so a rejected batch leaves no partial
// rows. It never commits or rolls back the caller's outer transaction.
func (store *Store) Bind(tx pgx.Tx) *TxAppender { return &TxAppender{store: store, tx: tx} }

func (store *Store) Append(ctx context.Context, records ...outbox.NewRecord) ([]outbox.Record, error) {
	return store.AppendBatch(ctx, outbox.AppendRequest{Records: records, DuplicateMode: store.config.DuplicateMode})
}

func (store *Store) AppendBatch(ctx context.Context, request outbox.AppendRequest) ([]outbox.Record, error) {
	if err := store.ensure(ctx); err != nil {
		return nil, err
	}
	tx, err := store.db.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	records, err := store.Bind(tx).AppendBatch(ctx, request)
	if err != nil {
		return nil, err
	}
	if err = tx.Commit(ctx); err != nil {
		return nil, err
	}
	return records, nil
}

func (appender *TxAppender) Append(ctx context.Context, records ...outbox.NewRecord) ([]outbox.Record, error) {
	return appender.AppendBatch(ctx, outbox.AppendRequest{Records: records, DuplicateMode: appender.store.config.DuplicateMode})
}

func (appender *TxAppender) AppendBatch(ctx context.Context, request outbox.AppendRequest) ([]outbox.Record, error) {
	if appender == nil || appender.store == nil || appender.tx == nil {
		return nil, fmt.Errorf("%w: transaction appender", outbox.ErrInvalidArgument)
	}
	if err := appender.store.ensure(ctx); err != nil {
		return nil, err
	}
	if !request.DuplicateMode.Valid() {
		return nil, fmt.Errorf("%w: duplicate mode", outbox.ErrInvalidArgument)
	}
	if len(request.Records) == 0 {
		return []outbox.Record{}, nil
	}
	if len(request.Records) > appender.store.config.Limits.MaxBatchSize {
		return nil, fmt.Errorf("%w: append batch exceeds %d", outbox.ErrInvalidArgument, appender.store.config.Limits.MaxBatchSize)
	}

	// A pgx pseudo-nested transaction maps to a savepoint and protects the
	// caller-owned transaction from partial insertion on a later duplicate.
	nested, err := appender.tx.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = nested.Rollback(context.Background()) }()
	now, err := databaseTime(ctx, nested)
	if err != nil {
		return nil, err
	}
	prepared := make([]outbox.Record, len(request.Records))
	for index, input := range request.Records {
		if input.ID == "" {
			input.ID, err = appender.store.config.IDGenerator.NewID()
			if err != nil {
				return nil, err
			}
		}
		prepared[index], err = outbox.PrepareRecord(input, now, appender.store.config.DefaultMaxAttempts, appender.store.config.Limits)
		if err != nil {
			return nil, err
		}
	}
	result := make([]outbox.Record, len(prepared))
	for index, record := range prepared {
		result[index], err = appender.insertOne(ctx, nested, record, request.DuplicateMode)
		if err != nil {
			return nil, err
		}
	}
	if err = nested.Commit(ctx); err != nil {
		return nil, err
	}
	return result, nil
}

func (appender *TxAppender) insertOne(ctx context.Context, tx pgx.Tx, record outbox.Record, mode outbox.DuplicateMode) (outbox.Record, error) {
	headers, err := marshalHeaders(record.Headers)
	if err != nil {
		return outbox.Record{}, err
	}
	query := `INSERT INTO ` + appender.store.table + ` (
		namespace, id, destination, message_type, aggregate_type, aggregate_id,
		ordering_key, idempotency_key, headers, payload, content_digest, state,
		attempts, max_attempts, available_at, created_at, updated_at, version
	) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,0,$13,$14,$15,$15,1)
	ON CONFLICT DO NOTHING RETURNING ` + recordColumnList
	inserted, scanErr := scanRecord(tx.QueryRow(ctx, query,
		appender.store.config.Namespace, record.ID, record.Destination, record.MessageType,
		nullable(record.AggregateType), nullable(record.AggregateID), nullable(record.OrderingKey),
		nullable(record.IdempotencyKey), headers, record.Payload, record.ContentDigest,
		outbox.StatePending, record.MaxAttempts, record.AvailableAt, record.CreatedAt,
	))
	if scanErr == nil {
		return inserted, nil
	}
	if !errors.Is(scanErr, pgx.ErrNoRows) {
		return outbox.Record{}, scanErr
	}
	if mode == outbox.RejectDuplicate {
		return outbox.Record{}, outbox.ErrDuplicateID
	}

	query = `SELECT ` + recordColumnList + ` FROM ` + appender.store.table + `
		WHERE namespace=$1 AND (id=$2 OR ($3::text IS NOT NULL AND idempotency_key=$3))
		ORDER BY CASE WHEN id=$2 THEN 0 ELSE 1 END LIMIT 2`
	rows, err := tx.Query(ctx, query, appender.store.config.Namespace, record.ID, nullable(record.IdempotencyKey))
	if err != nil {
		return outbox.Record{}, err
	}
	defer rows.Close()
	var matches []outbox.Record
	for rows.Next() {
		match, rowErr := scanRecord(rows)
		if rowErr != nil {
			return outbox.Record{}, rowErr
		}
		matches = append(matches, match)
	}
	if err = rows.Err(); err != nil {
		return outbox.Record{}, err
	}
	if len(matches) == 0 {
		return outbox.Record{}, outbox.ErrConflict
	}
	for _, match := range matches {
		if match.ContentDigest != record.ContentDigest {
			return outbox.Record{}, outbox.ErrConflict
		}
	}
	if len(matches) == 2 && matches[0].ID != matches[1].ID {
		return outbox.Record{}, outbox.ErrConflict
	}
	return matches[0], nil
}

func marshalHeaders(headers map[string]string) ([]byte, error) {
	if headers == nil {
		return []byte(`{}`), nil
	}
	encoded, err := json.Marshal(headers)
	if err != nil {
		return nil, fmt.Errorf("encode outbox headers: %w", err)
	}
	return encoded, nil
}

func databaseTime(ctx context.Context, queryer interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}) (time.Time, error) {
	var now time.Time
	if err := queryer.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&now); err != nil {
		return time.Time{}, err
	}
	now = outbox.CanonicalTime(now)
	if err := outbox.ValidateTimestamp("storage_time", now); err != nil {
		return time.Time{}, err
	}
	return now, nil
}

var _ outbox.IBatchAppender = (*TxAppender)(nil)
