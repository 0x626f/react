package outbox

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"time"
)

func (store *RedisStore) Claim(ctx context.Context, request ClaimRequest) ([]Record, error) {
	if err := store.ensure(ctx); err != nil {
		return nil, err
	}
	if err := ValidateLeaseOwner(request.Owner, store.config.Limits); err != nil {
		return nil, err
	}
	if request.Limit < 1 || request.Limit > store.config.Limits.MaxClaimBatchSize {
		return nil, fmt.Errorf("%w: claim limit", ErrInvalidArgument)
	}
	if err := ValidateLeaseDuration("lease_duration", request.LeaseDuration, store.config.MaxLeaseDuration); err != nil {
		return nil, err
	}
	if request.RecoveryLimit == 0 {
		request.RecoveryLimit = request.Limit
	}
	if request.RecoveryLimit < 1 || request.RecoveryLimit > store.config.Limits.MaxClaimBatchSize {
		return nil, fmt.Errorf("%w: recovery limit", ErrInvalidArgument)
	}
	if len(request.Destinations) > 16 {
		return nil, fmt.Errorf("%w: at most 16 destination filters are supported", ErrInvalidArgument)
	}
	destinations := make([]string, 0, len(request.Destinations))
	seenDestinations := make(map[string]struct{}, len(request.Destinations))
	for _, destination := range request.Destinations {
		if err := ValidateDestination(destination, store.config.Limits); err != nil {
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
		token, err := generateLeaseToken()
		if err != nil {
			return nil, err
		}
		if err = ValidateLeaseToken(token, store.config.Limits); err != nil {
			return nil, err
		}
		if _, exists := seenTokens[token]; exists {
			return nil, fmt.Errorf("%w: duplicate lease token", ErrInvalidArgument)
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
		return nil, fmt.Errorf("%w: lease deadline exceeds the portable timestamp range", ErrInvalidArgument)
	}
	if err != nil || code != 0 {
		return nil, fmt.Errorf("unexpected Redis claim result code %d: %w", code, err)
	}
	result := make([]Record, 0, len(values)-1)
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

func (store *RedisStore) Renew(ctx context.Context, lease LeaseRef, until time.Time) error {
	if err := store.ensure(ctx); err != nil {
		return err
	}
	if err := validateLease(lease); err != nil {
		return err
	}
	until = CanonicalTime(until)
	if err := ValidateTimestamp("lease_until", until); err != nil {
		return err
	}
	code, err := store.runMutation(ctx, renewScript, lease.ID, lease.Owner, lease.Token, lease.Version, until.UnixMicro(), store.config.MaxLeaseDuration.Microseconds())
	if code == -5 {
		return fmt.Errorf("%w: renewal deadline", ErrInvalidArgument)
	}
	return mapLeaseMutation(code, err)
}

func (store *RedisStore) Acknowledge(ctx context.Context, lease LeaseRef, _ DeliveryResult) error {
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
		return ErrConflict
	}
	return mapLeaseMutation(code, nil)
}

func (store *RedisStore) Retry(ctx context.Context, lease LeaseRef, retry RetryRequest) error {
	if err := store.ensure(ctx); err != nil {
		return err
	}
	if err := validateLease(lease); err != nil {
		return err
	}
	retry.AvailableAt = CanonicalTime(retry.AvailableAt)
	if err := ValidateTimestamp("available_at", retry.AvailableAt); err != nil {
		return err
	}
	failure := BoundFailure(retry.Failure, store.config.Limits)
	code, err := store.runMutation(ctx, retryScript, lease.ID, lease.Owner, lease.Token, lease.Version, retry.AvailableAt.UnixMicro(), failure.Code, failure.Message)
	return mapLeaseMutation(code, err)
}

func (store *RedisStore) DeadLetter(ctx context.Context, lease LeaseRef, failure Failure) error {
	if err := store.ensure(ctx); err != nil {
		return err
	}
	if err := validateLease(lease); err != nil {
		return err
	}
	failure = BoundFailure(failure, store.config.Limits)
	code, err := store.runMutation(ctx, deadLetterScript, lease.ID, lease.Owner, lease.Token, lease.Version, failure.Code, failure.Message)
	return mapLeaseMutation(code, err)
}

func (store *RedisStore) Release(ctx context.Context, lease LeaseRef, availableAt time.Time) error {
	if err := store.ensure(ctx); err != nil {
		return err
	}
	if err := validateLease(lease); err != nil {
		return err
	}
	availableAt = CanonicalTime(availableAt)
	if err := ValidateTimestamp("available_at", availableAt); err != nil {
		return err
	}
	code, err := store.runMutation(ctx, releaseScript, lease.ID, lease.Owner, lease.Token, lease.Version, availableAt.UnixMicro())
	return mapLeaseMutation(code, err)
}

func (store *RedisStore) runMutation(ctx context.Context, script luaScript, args ...any) (int64, error) {
	for index, argument := range args {
		if id, ok := argument.(ID); ok {
			args[index] = string(id)
		}
	}
	value, err := store.runScript(ctx, script, store.keys.ScriptKeys(), args...)
	if err != nil {
		return 0, err
	}
	return resultCode(value)
}

func validateLease(lease LeaseRef) error {
	if lease.ID == "" || lease.Owner == "" || lease.Token == "" || lease.Version == 0 {
		return ErrLeaseLost
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
		return ErrLeaseLost
	default:
		return mapMutationCode(code)
	}
}
