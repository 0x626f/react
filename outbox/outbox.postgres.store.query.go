package outbox

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

func (store *PostgresStore) Get(ctx context.Context, id ID) (Record, error) {
	if err := store.ensure(ctx); err != nil {
		return Record{}, err
	}
	if err := ValidateID(id, store.config.Limits); err != nil {
		return Record{}, err
	}
	record, err := scanRecord(store.db.QueryRow(ctx, `SELECT `+recordColumnList+` FROM `+store.table+` WHERE namespace=$1 AND id=$2`, store.config.Namespace, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return Record{}, ErrNotFound
	}
	return record, err
}

func (store *PostgresStore) Find(ctx context.Context, query Query) (Page, error) {
	if err := store.ensure(ctx); err != nil {
		return Page{}, err
	}
	query, cursor, err := NormalizeQuery(query, store.config.Limits)
	if err != nil {
		return Page{}, err
	}
	conditions := []string{"namespace=$1"}
	args := []any{store.config.Namespace}
	add := func(condition string, values ...any) {
		for index := range values {
			condition = strings.ReplaceAll(condition, fmt.Sprintf("?%d", index+1), fmt.Sprintf("$%d", len(args)+index+1))
		}
		conditions = append(conditions, condition)
		args = append(args, values...)
	}
	if len(query.IDs) > 0 {
		ids := make([]string, len(query.IDs))
		for index, id := range query.IDs {
			ids[index] = string(id)
		}
		add(`id = ANY(?1::text[])`, ids)
	}
	if len(query.States) > 0 {
		add(`state = ANY(?1::text[])`, postgresStates(query.States))
	}
	if len(query.Destinations) > 0 {
		add(`destination = ANY(?1::text[])`, query.Destinations)
	}
	if len(query.MessageTypes) > 0 {
		add(`message_type = ANY(?1::text[])`, query.MessageTypes)
	}
	if query.AggregateType != "" {
		add(`aggregate_type=?1`, query.AggregateType)
	}
	if query.AggregateID != "" {
		add(`aggregate_id=?1`, query.AggregateID)
	}
	if query.OrderingKey != "" {
		add(`ordering_key=?1`, query.OrderingKey)
	}
	if query.IdempotencyKey != "" {
		add(`idempotency_key=?1`, query.IdempotencyKey)
	}
	if query.CreatedAt.From != nil {
		add(`created_at>=?1`, *query.CreatedAt.From)
	}
	if query.CreatedAt.To != nil {
		add(`created_at<=?1`, *query.CreatedAt.To)
	}
	if query.AvailableAt.From != nil {
		add(`available_at>=?1`, *query.AvailableAt.From)
	}
	if query.AvailableAt.To != nil {
		add(`available_at<=?1`, *query.AvailableAt.To)
	}
	sortColumn := "created_at"
	if query.Sort == SortAvailableAt {
		sortColumn = "available_at"
	}
	direction, comparison := "ASC", ">"
	if query.Direction == SortDescending {
		direction, comparison = "DESC", "<"
	}
	if cursor.Version != 0 {
		add(fmt.Sprintf(`(%s,id) %s (?1,?2)`, sortColumn, comparison), time.UnixMicro(cursor.Micros).UTC(), cursor.ID)
	}
	args = append(args, query.Limit+1)
	sql := `SELECT ` + recordColumnList + ` FROM ` + store.table + ` WHERE ` + strings.Join(conditions, " AND ") + fmt.Sprintf(` ORDER BY %s %s, id %s LIMIT $%d`, sortColumn, direction, direction, len(args))
	rows, err := store.db.Query(ctx, sql, args...)
	if err != nil {
		return Page{}, err
	}
	defer rows.Close()
	records := make([]Record, 0, query.Limit+1)
	for rows.Next() {
		record, rowErr := scanRecord(rows)
		if rowErr != nil {
			return Page{}, rowErr
		}
		records = append(records, record)
	}
	if err = rows.Err(); err != nil {
		return Page{}, err
	}
	page := Page{}
	if len(records) > query.Limit {
		page.Records = records[:query.Limit]
		page.NextCursor, err = CursorForRecord(page.Records[len(page.Records)-1], query.Sort, query.Direction)
	} else {
		page.Records = records
	}
	return page, err
}

func (store *PostgresStore) Cancel(ctx context.Context, id ID, reason string) error {
	if err := store.ensure(ctx); err != nil {
		return err
	}
	if err := ValidateID(id, store.config.Limits); err != nil {
		return err
	}
	failure := BoundFailure(Failure{Message: reason}, store.config.Limits)
	tag, err := store.db.Exec(ctx, `UPDATE `+store.table+` SET state='cancelled', cancelled_at=clock_timestamp(),
		last_error_code='cancelled', last_error_message=$3, updated_at=clock_timestamp(), version=version+1
		WHERE namespace=$1 AND id=$2 AND state='pending'`, store.config.Namespace, id, nullable(failure.Message))
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 1 {
		return nil
	}
	state, err := diagnoseState(ctx, store.db, store.table, store.config.Namespace, id)
	if err != nil {
		return err
	}
	if state == StateCancelled {
		return nil
	}
	return ErrInvalidTransition
}

func (store *PostgresStore) Reschedule(ctx context.Context, id ID, availableAt time.Time) error {
	if err := store.ensure(ctx); err != nil {
		return err
	}
	if err := ValidateID(id, store.config.Limits); err != nil {
		return err
	}
	availableAt = CanonicalTime(availableAt)
	if err := ValidateTimestamp("available_at", availableAt); err != nil {
		return err
	}
	tag, err := store.db.Exec(ctx, `UPDATE `+store.table+` SET available_at=$3,
		updated_at=clock_timestamp(), version=version+1
		WHERE namespace=$1 AND id=$2 AND state='pending' AND available_at IS DISTINCT FROM $3`, store.config.Namespace, id, availableAt)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 1 {
		return nil
	}
	state, err := diagnoseState(ctx, store.db, store.table, store.config.Namespace, id)
	if err != nil {
		return err
	}
	if state != StatePending {
		return ErrInvalidTransition
	}
	return nil
}

func (store *PostgresStore) Requeue(ctx context.Context, id ID, options RequeueOptions) error {
	if err := store.ensure(ctx); err != nil {
		return err
	}
	if err := ValidateID(id, store.config.Limits); err != nil {
		return err
	}
	if options.MaxAttempts < 0 || options.MaxAttempts > store.config.Limits.MaxAttempts {
		return fmt.Errorf("%w: max attempts", ErrInvalidArgument)
	}
	availableAt := CanonicalTime(options.AvailableAt)
	if availableAt.IsZero() {
		var err error
		availableAt, err = databaseTime(ctx, store.db)
		if err != nil {
			return err
		}
	}
	if err := ValidateTimestamp("available_at", availableAt); err != nil {
		return err
	}
	tag, err := store.db.Exec(ctx, `UPDATE `+store.table+` SET state='pending', available_at=$3,
		attempts=CASE WHEN $4 THEN 0 ELSE attempts END,
		max_attempts=CASE WHEN $5::integer > 0 THEN $5 ELSE max_attempts END,
		last_error_code=NULL, last_error_message=NULL, dead_at=NULL, updated_at=clock_timestamp(), version=version+1
		WHERE namespace=$1 AND id=$2 AND state='dead'
		AND (CASE WHEN $5::integer > 0 THEN $5 ELSE max_attempts END) >
			(CASE WHEN $4 THEN 0 ELSE attempts END)`, store.config.Namespace, id, availableAt, options.ResetAttempts, options.MaxAttempts)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 1 {
		return nil
	}
	state, err := diagnoseState(ctx, store.db, store.table, store.config.Namespace, id)
	if err != nil {
		return err
	}
	if state != StateDead {
		return ErrInvalidTransition
	}
	return fmt.Errorf("%w: requeue requires ResetAttempts or MaxAttempts greater than preserved attempts", ErrInvalidArgument)
}

func (store *PostgresStore) Purge(ctx context.Context, request PurgeRequest) (int, error) {
	if err := store.ensure(ctx); err != nil {
		return 0, err
	}
	request, err := NormalizePurgeRequest(request, store.config.Limits)
	if err != nil {
		return 0, err
	}
	tag, err := store.db.Exec(ctx, `WITH victims AS (
		SELECT namespace,id FROM `+store.table+` WHERE namespace=$1 AND state=ANY($2::text[])
		AND COALESCE(delivered_at,dead_at,cancelled_at) < $3
		ORDER BY COALESCE(delivered_at,dead_at,cancelled_at),id FOR UPDATE SKIP LOCKED LIMIT $4
	) DELETE FROM `+store.table+` target USING victims
	WHERE target.namespace=victims.namespace AND target.id=victims.id`, store.config.Namespace, postgresStates(request.States), request.Before, request.Limit)
	if err != nil {
		return 0, err
	}
	count := int(tag.RowsAffected())
	return count, nil
}

func (store *PostgresStore) Backlog(ctx context.Context) (Backlog, error) {
	if err := store.ensure(ctx); err != nil {
		return Backlog{}, err
	}
	var backlog Backlog
	err := store.db.QueryRow(ctx, `SELECT
		COUNT(*) FILTER (WHERE state='pending'), COUNT(*) FILTER (WHERE state='leased'),
		COUNT(*) FILTER (WHERE state='dead'), MIN(available_at) FILTER (WHERE state='pending' AND available_at <= clock_timestamp())
		FROM `+store.table+` WHERE namespace=$1`, store.config.Namespace).Scan(&backlog.Pending, &backlog.Leased, &backlog.Dead, &backlog.OldestDueAt)
	backlog.OldestDueAt = canonicalPointer(backlog.OldestDueAt)
	return backlog, err
}

func (store *PostgresStore) Health(ctx context.Context) Health {
	backlog, err := store.Backlog(ctx)
	if err != nil {
		return Health{Ready: false, StorageAvailable: false, DurabilitySafe: true, Message: err.Error()}
	}
	return Health{Ready: true, StorageAvailable: true, DurabilitySafe: true, Backlog: backlog}
}

var _ IBacklogReader = (*PostgresStore)(nil)
var _ IHealthChecker = (*PostgresStore)(nil)
