package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/0x626f/react/outbox"
	"github.com/jackc/pgx/v5"
)

func (store *Store) Claim(ctx context.Context, request outbox.ClaimRequest) ([]outbox.Record, error) {
	if err := store.ensure(ctx); err != nil {
		return nil, err
	}
	if err := store.validateClaim(request); err != nil {
		return nil, err
	}
	if request.RecoveryLimit == 0 {
		request.RecoveryLimit = request.Limit
	}
	tokens := make([]string, request.Limit)
	seenTokens := make(map[string]struct{}, request.Limit)
	for index := range tokens {
		token, tokenErr := store.config.TokenGenerator.NewToken()
		if tokenErr != nil {
			return nil, tokenErr
		}
		if tokenErr = outbox.ValidateLeaseToken(token, store.config.Limits); tokenErr != nil {
			return nil, tokenErr
		}
		if _, duplicate := seenTokens[token]; duplicate {
			return nil, fmt.Errorf("%w: token generator returned a duplicate token", outbox.ErrInvalidArgument)
		}
		seenTokens[token] = struct{}{}
		tokens[index] = token
	}
	tx, err := store.db.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	now, err := databaseTime(ctx, tx)
	if err != nil {
		return nil, err
	}
	if err = store.recoverExpired(ctx, tx, now, request.RecoveryLimit); err != nil {
		return nil, err
	}

	query := `SELECT id FROM ` + store.table + `
		WHERE namespace=$1 AND state='pending' AND available_at <= $2 AND attempts < max_attempts`
	args := []any{store.config.Namespace, now}
	if len(request.Destinations) > 0 {
		query += ` AND destination = ANY($3::text[])`
		args = append(args, request.Destinations)
	}
	args = append(args, request.Limit)
	query += fmt.Sprintf(` ORDER BY available_at, created_at, id FOR UPDATE SKIP LOCKED LIMIT $%d`, len(args))
	rows, err := tx.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	ids := make([]outbox.ID, 0, request.Limit)
	for rows.Next() {
		var id outbox.ID
		if err = rows.Scan(&id); err != nil {
			rows.Close()
			return nil, err
		}
		ids = append(ids, id)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, err
	}

	leaseUntil := outbox.CanonicalTime(now.Add(request.LeaseDuration))
	if err = outbox.ValidateTimestamp("lease_until", leaseUntil); err != nil {
		return nil, err
	}
	claimed := make([]outbox.Record, 0, len(ids))
	for index, id := range ids {
		token := tokens[index]
		update := `UPDATE ` + store.table + ` SET
			state='leased', attempts=attempts+1, lease_owner=$3, lease_token=$4,
			lease_until=$5, updated_at=$2, version=version+1,
			completed_lease_owner=NULL, completed_lease_token=NULL, completed_lease_version=NULL
			WHERE namespace=$1 AND id=$6 AND state='pending' AND available_at <= $2 AND attempts < max_attempts
			RETURNING ` + recordColumnList
		record, updateErr := scanRecord(tx.QueryRow(ctx, update, store.config.Namespace, now, request.Owner, token, leaseUntil, id))
		if updateErr != nil {
			return nil, updateErr
		}
		claimed = append(claimed, record)
	}
	if err = tx.Commit(ctx); err != nil {
		return nil, err
	}
	return claimed, nil
}

func (store *Store) validateClaim(request outbox.ClaimRequest) error {
	if err := outbox.ValidateLeaseOwner(request.Owner, store.config.Limits); err != nil {
		return err
	}
	if request.Limit < 1 || request.Limit > store.config.Limits.MaxClaimBatchSize {
		return fmt.Errorf("%w: claim limit", outbox.ErrInvalidArgument)
	}
	if err := outbox.ValidateLeaseDuration("lease_duration", request.LeaseDuration, store.config.MaxLeaseDuration); err != nil {
		return err
	}
	if request.RecoveryLimit < 0 || request.RecoveryLimit > store.config.Limits.MaxClaimBatchSize {
		return fmt.Errorf("%w: recovery limit", outbox.ErrInvalidArgument)
	}
	if len(request.Destinations) > store.config.Limits.MaxQueryValues {
		return fmt.Errorf("%w: destination filters", outbox.ErrInvalidArgument)
	}
	for _, destination := range request.Destinations {
		if err := outbox.ValidateDestination(destination, store.config.Limits); err != nil {
			return err
		}
	}
	return nil
}

func (store *Store) recoverExpired(ctx context.Context, tx pgx.Tx, now time.Time, limit int) error {
	rows, err := tx.Query(ctx, `SELECT id, attempts, max_attempts FROM `+store.table+`
		WHERE namespace=$1 AND state='leased' AND lease_until <= $2
		ORDER BY lease_until, id FOR UPDATE SKIP LOCKED LIMIT $3`, store.config.Namespace, now, limit)
	if err != nil {
		return err
	}
	type expiredRecord struct {
		id                outbox.ID
		attempts, maximum int
	}
	expired := make([]expiredRecord, 0, limit)
	for rows.Next() {
		var record expiredRecord
		if err = rows.Scan(&record.id, &record.attempts, &record.maximum); err != nil {
			rows.Close()
			return err
		}
		expired = append(expired, record)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	for _, record := range expired {
		if record.attempts >= record.maximum {
			_, err = tx.Exec(ctx, `UPDATE `+store.table+` SET state='dead', dead_at=$2,
				lease_owner=NULL, lease_token=NULL, lease_until=NULL, updated_at=$2,
				last_error_code='lease_expired_exhausted', last_error_message='lease expired after the final delivery attempt', version=version+1
				WHERE namespace=$1 AND id=$3 AND state='leased' AND lease_until <= $2`, store.config.Namespace, now, record.id)
		} else {
			_, err = tx.Exec(ctx, `UPDATE `+store.table+` SET state='pending', available_at=$2,
				lease_owner=NULL, lease_token=NULL, lease_until=NULL, updated_at=$2,
				last_error_code='lease_expired', last_error_message='delivery lease expired', version=version+1
				WHERE namespace=$1 AND id=$3 AND state='leased' AND lease_until <= $2`, store.config.Namespace, now, record.id)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func (store *Store) Renew(ctx context.Context, lease outbox.LeaseRef, until time.Time) error {
	if err := store.ensure(ctx); err != nil {
		return err
	}
	if err := validateLeaseRef(lease); err != nil {
		return err
	}
	until = outbox.CanonicalTime(until)
	if err := outbox.ValidateTimestamp("lease_until", until); err != nil {
		return err
	}
	now, err := databaseTime(ctx, store.db)
	if err != nil {
		return err
	}
	if !until.After(now) || until.After(now.Add(store.config.MaxLeaseDuration)) {
		return fmt.Errorf("%w: renewal deadline", outbox.ErrInvalidArgument)
	}
	tag, err := store.db.Exec(ctx, `UPDATE `+store.table+` SET lease_until=$6, updated_at=clock_timestamp()
		WHERE namespace=$1 AND id=$2 AND state='leased' AND lease_owner=$3 AND lease_token=$4 AND version=$5 AND lease_until > clock_timestamp()`,
		store.config.Namespace, lease.ID, lease.Owner, lease.Token, lease.Version, until)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return outbox.ErrLeaseLost
	}
	return nil
}

func (store *Store) Acknowledge(ctx context.Context, lease outbox.LeaseRef, _ outbox.DeliveryResult) error {
	if err := store.ensure(ctx); err != nil {
		return err
	}
	if err := validateLeaseRef(lease); err != nil {
		return err
	}
	tag, err := store.db.Exec(ctx, `UPDATE `+store.table+` SET state='delivered', delivered_at=clock_timestamp(),
		updated_at=clock_timestamp(), completed_lease_owner=lease_owner, completed_lease_token=lease_token,
		completed_lease_version=version, lease_owner=NULL, lease_token=NULL, lease_until=NULL, version=version+1
		WHERE namespace=$1 AND id=$2 AND state='leased' AND lease_owner=$3 AND lease_token=$4 AND version=$5 AND lease_until > clock_timestamp()`,
		store.config.Namespace, lease.ID, lease.Owner, lease.Token, lease.Version)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 1 {
		return nil
	}
	var state outbox.State
	var owner, token *string
	var version *uint64
	err = store.db.QueryRow(ctx, `SELECT state, completed_lease_owner, completed_lease_token, completed_lease_version FROM `+store.table+` WHERE namespace=$1 AND id=$2`, store.config.Namespace, lease.ID).Scan(&state, &owner, &token, &version)
	if errors.Is(err, pgx.ErrNoRows) {
		return outbox.ErrLeaseLost
	}
	if err != nil {
		return err
	}
	if state == outbox.StateDelivered && owner != nil && token != nil && version != nil && *owner == lease.Owner && *token == lease.Token && *version == lease.Version {
		return nil
	}
	if state == outbox.StateDelivered {
		return outbox.ErrConflict
	}
	return outbox.ErrLeaseLost
}

func (store *Store) Retry(ctx context.Context, lease outbox.LeaseRef, retry outbox.RetryRequest) error {
	if err := store.ensure(ctx); err != nil {
		return err
	}
	if err := validateLeaseRef(lease); err != nil {
		return err
	}
	availableAt := outbox.CanonicalTime(retry.AvailableAt)
	if err := outbox.ValidateTimestamp("available_at", availableAt); err != nil {
		return err
	}
	failure := outbox.BoundFailure(retry.Failure, store.config.Limits)
	tag, err := store.db.Exec(ctx, `UPDATE `+store.table+` SET
		state=CASE WHEN attempts >= max_attempts THEN 'dead' ELSE 'pending' END,
		available_at=CASE WHEN attempts >= max_attempts THEN available_at ELSE GREATEST($6, clock_timestamp()) END,
		dead_at=CASE WHEN attempts >= max_attempts THEN clock_timestamp() ELSE NULL END,
		last_error_code=$7, last_error_message=$8, lease_owner=NULL, lease_token=NULL,
		lease_until=NULL, updated_at=clock_timestamp(), version=version+1
		WHERE namespace=$1 AND id=$2 AND state='leased' AND lease_owner=$3 AND lease_token=$4 AND version=$5 AND lease_until > clock_timestamp()`,
		store.config.Namespace, lease.ID, lease.Owner, lease.Token, lease.Version, availableAt, nullable(failure.Code), nullable(failure.Message))
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return outbox.ErrLeaseLost
	}
	return nil
}

func (store *Store) DeadLetter(ctx context.Context, lease outbox.LeaseRef, failure outbox.Failure) error {
	if err := store.ensure(ctx); err != nil {
		return err
	}
	if err := validateLeaseRef(lease); err != nil {
		return err
	}
	failure = outbox.BoundFailure(failure, store.config.Limits)
	tag, err := store.db.Exec(ctx, `UPDATE `+store.table+` SET state='dead', dead_at=clock_timestamp(),
		last_error_code=$6, last_error_message=$7, lease_owner=NULL, lease_token=NULL,
		lease_until=NULL, updated_at=clock_timestamp(), version=version+1
		WHERE namespace=$1 AND id=$2 AND state='leased' AND lease_owner=$3 AND lease_token=$4 AND version=$5 AND lease_until > clock_timestamp()`,
		store.config.Namespace, lease.ID, lease.Owner, lease.Token, lease.Version, nullable(failure.Code), nullable(failure.Message))
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return outbox.ErrLeaseLost
	}
	return nil
}

func (store *Store) Release(ctx context.Context, lease outbox.LeaseRef, availableAt time.Time) error {
	if err := store.ensure(ctx); err != nil {
		return err
	}
	if err := validateLeaseRef(lease); err != nil {
		return err
	}
	availableAt = outbox.CanonicalTime(availableAt)
	if err := outbox.ValidateTimestamp("available_at", availableAt); err != nil {
		return err
	}
	tag, err := store.db.Exec(ctx, `UPDATE `+store.table+` SET
		state=CASE WHEN attempts >= max_attempts THEN 'dead' ELSE 'pending' END,
		available_at=CASE WHEN attempts >= max_attempts THEN available_at ELSE GREATEST($6, clock_timestamp()) END,
		dead_at=CASE WHEN attempts >= max_attempts THEN clock_timestamp() ELSE NULL END,
		last_error_code=CASE WHEN attempts >= max_attempts THEN 'released_exhausted' ELSE last_error_code END,
		last_error_message=CASE WHEN attempts >= max_attempts THEN 'delivery lease released after the final attempt' ELSE last_error_message END,
		lease_owner=NULL, lease_token=NULL, lease_until=NULL, updated_at=clock_timestamp(), version=version+1
		WHERE namespace=$1 AND id=$2 AND state='leased' AND lease_owner=$3 AND lease_token=$4 AND version=$5 AND lease_until > clock_timestamp()`,
		store.config.Namespace, lease.ID, lease.Owner, lease.Token, lease.Version, availableAt)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return outbox.ErrLeaseLost
	}
	return nil
}

func validateLeaseRef(lease outbox.LeaseRef) error {
	if lease.ID == "" || lease.Owner == "" || lease.Token == "" || lease.Version == 0 {
		return outbox.ErrLeaseLost
	}
	return nil
}
