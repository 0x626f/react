package redis

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"time"

	"github.com/0x626f/react/outbox"
)

func (store *Store) Claim(ctx context.Context, request outbox.ClaimRequest) ([]outbox.Record, error) {
	if err := store.ensure(ctx); err != nil {
		return nil, err
	}
	if err := outbox.ValidateLeaseOwner(request.Owner, store.config.Limits); err != nil {
		return nil, err
	}
	if request.Limit < 1 || request.Limit > store.config.Limits.MaxClaimBatchSize {
		return nil, fmt.Errorf("%w: claim limit", outbox.ErrInvalidArgument)
	}
	if err := outbox.ValidateLeaseDuration("lease_duration", request.LeaseDuration, store.config.MaxLeaseDuration); err != nil {
		return nil, err
	}
	if request.RecoveryLimit == 0 {
		request.RecoveryLimit = request.Limit
	}
	if request.RecoveryLimit < 1 || request.RecoveryLimit > store.config.Limits.MaxClaimBatchSize {
		return nil, fmt.Errorf("%w: recovery limit", outbox.ErrInvalidArgument)
	}
	if len(request.Destinations) > 16 {
		return nil, fmt.Errorf("%w: at most 16 destination filters are supported", outbox.ErrInvalidArgument)
	}
	destinations := make([]string, 0, len(request.Destinations))
	seenDestinations := make(map[string]struct{}, len(request.Destinations))
	for _, destination := range request.Destinations {
		if err := outbox.ValidateDestination(destination, store.config.Limits); err != nil {
			return nil, err
		}
		encoded := base64.RawURLEncoding.EncodeToString([]byte(destination))
		if _, exists := seenDestinations[encoded]; exists {
			continue
		}
		seenDestinations[encoded] = struct{}{}
		destinations = append(destinations, encoded)
	}
	tokens := make([]string, request.Limit)
	seenTokens := make(map[string]struct{}, request.Limit)
	for index := range tokens {
		token, err := store.config.TokenGenerator.NewToken()
		if err != nil {
			return nil, err
		}
		if err = outbox.ValidateLeaseToken(token, store.config.Limits); err != nil {
			return nil, err
		}
		if _, exists := seenTokens[token]; exists {
			return nil, fmt.Errorf("%w: duplicate lease token", outbox.ErrInvalidArgument)
		}
		seenTokens[token] = struct{}{}
		tokens[index] = token
	}
	encodedTokens, _ := json.Marshal(tokens)
	encodedDestinations, _ := json.Marshal(destinations)
	value, err := store.runScript(ctx, claimScript, store.keys.ScriptKeys(), request.Owner, request.Limit, request.LeaseDuration.Microseconds(), request.RecoveryLimit, string(encodedTokens), string(encodedDestinations), store.config.MaxClaimResponseBytes)
	if err != nil {
		return nil, err
	}
	values, err := resultArray(value)
	if err != nil || len(values) == 0 {
		return nil, fmt.Errorf("decode Redis claim result: %w", err)
	}
	code, err := resultCode(values[0])
	if code == -5 {
		return nil, fmt.Errorf("%w: lease deadline exceeds the portable timestamp range", outbox.ErrInvalidArgument)
	}
	if err != nil || code != 0 {
		return nil, fmt.Errorf("unexpected Redis claim result code %d: %w", code, err)
	}
	result := make([]outbox.Record, 0, len(values)-1)
	for _, raw := range values[1:] {
		text, ok := raw.(string)
		if !ok {
			return nil, fmt.Errorf("unexpected Redis claimed record type %T", raw)
		}
		record, decodeErr := decodeRecord(text)
		if decodeErr != nil {
			return nil, decodeErr
		}
		result = append(result, record)
	}
	return result, nil
}

func (store *Store) Renew(ctx context.Context, lease outbox.LeaseRef, until time.Time) error {
	if err := store.ensure(ctx); err != nil {
		return err
	}
	if err := validateLease(lease); err != nil {
		return err
	}
	until = outbox.CanonicalTime(until)
	if err := outbox.ValidateTimestamp("lease_until", until); err != nil {
		return err
	}
	code, err := store.runMutation(ctx, renewScript, lease.ID, lease.Owner, lease.Token, lease.Version, until.UnixMicro(), store.config.MaxLeaseDuration.Microseconds())
	if code == -5 {
		return fmt.Errorf("%w: renewal deadline", outbox.ErrInvalidArgument)
	}
	return mapLeaseMutation(code, err)
}

func (store *Store) Acknowledge(ctx context.Context, lease outbox.LeaseRef, _ outbox.DeliveryResult) error {
	if err := store.ensure(ctx); err != nil {
		return err
	}
	if err := validateLease(lease); err != nil {
		return err
	}
	code, err := store.runMutation(ctx, acknowledgeScript, lease.ID, lease.Owner, lease.Token, lease.Version)
	if err != nil {
		return err
	}
	if code == -3 {
		return outbox.ErrConflict
	}
	return mapLeaseMutation(code, nil)
}

func (store *Store) Retry(ctx context.Context, lease outbox.LeaseRef, retry outbox.RetryRequest) error {
	if err := store.ensure(ctx); err != nil {
		return err
	}
	if err := validateLease(lease); err != nil {
		return err
	}
	retry.AvailableAt = outbox.CanonicalTime(retry.AvailableAt)
	if err := outbox.ValidateTimestamp("available_at", retry.AvailableAt); err != nil {
		return err
	}
	failure := outbox.BoundFailure(retry.Failure, store.config.Limits)
	code, err := store.runMutation(ctx, retryScript, lease.ID, lease.Owner, lease.Token, lease.Version, retry.AvailableAt.UnixMicro(), failure.Code, failure.Message)
	return mapLeaseMutation(code, err)
}

func (store *Store) DeadLetter(ctx context.Context, lease outbox.LeaseRef, failure outbox.Failure) error {
	if err := store.ensure(ctx); err != nil {
		return err
	}
	if err := validateLease(lease); err != nil {
		return err
	}
	failure = outbox.BoundFailure(failure, store.config.Limits)
	code, err := store.runMutation(ctx, deadLetterScript, lease.ID, lease.Owner, lease.Token, lease.Version, failure.Code, failure.Message)
	return mapLeaseMutation(code, err)
}

func (store *Store) Release(ctx context.Context, lease outbox.LeaseRef, availableAt time.Time) error {
	if err := store.ensure(ctx); err != nil {
		return err
	}
	if err := validateLease(lease); err != nil {
		return err
	}
	availableAt = outbox.CanonicalTime(availableAt)
	if err := outbox.ValidateTimestamp("available_at", availableAt); err != nil {
		return err
	}
	code, err := store.runMutation(ctx, releaseScript, lease.ID, lease.Owner, lease.Token, lease.Version, availableAt.UnixMicro())
	return mapLeaseMutation(code, err)
}

func (store *Store) runMutation(ctx context.Context, script luaScript, args ...any) (int64, error) {
	for index, argument := range args {
		if id, ok := argument.(outbox.ID); ok {
			args[index] = string(id)
		}
	}
	value, err := store.runScript(ctx, script, store.keys.ScriptKeys(), args...)
	if err != nil {
		return 0, err
	}
	return resultCode(value)
}

func validateLease(lease outbox.LeaseRef) error {
	if lease.ID == "" || lease.Owner == "" || lease.Token == "" || lease.Version == 0 {
		return outbox.ErrLeaseLost
	}
	return nil
}

func mapLeaseMutation(code int64, err error) error {
	if err != nil {
		return err
	}
	switch code {
	case 0, 1:
		return nil
	case -1, -2:
		return outbox.ErrLeaseLost
	default:
		return mapMutationCode(code)
	}
}
