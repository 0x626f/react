// RedisStore implements storage with bounded, atomic Redis Lua scripts. It
// treats Redis as authoritative storage, not as a cache. Production
// deployments must deliberately configure persistence, noeviction, backups,
// replication, and failover.
package outbox

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

// RedisStore implements the portable contracts with an application-owned client.
type RedisStore struct {
	client     goredis.UniversalClient
	config     RedisConfig
	keys       RedisKeys
	durability atomic.Pointer[RedisDurabilityReport]
	closed     atomic.Bool
}

type luaScript struct {
	source string
	script *goredis.Script
}

func newLuaScript(source string) luaScript {
	return luaScript{source: source, script: goredis.NewScript(source)}
}

// NewRedisStore validates dependencies, pings Redis, and performs startup durability checks.
func NewRedisStore(ctx context.Context, client goredis.UniversalClient, config RedisConfig) (*RedisStore, error) {
	if client == nil {
		return nil, fmt.Errorf("%w: Redis client is required", ErrInvalidArgument)
	}
	config, err := config.normalized()
	if err != nil {
		return nil, err
	}
	keys, _ := NewRedisKeys(config.Namespace)
	store := &RedisStore{client: client, config: config, keys: keys}
	if err = client.Ping(ctx).Err(); err != nil {
		return nil, err
	}
	report, err := store.CheckDurability(ctx)
	store.durability.Store(&report)
	if err != nil {
		return nil, err
	}
	return store, nil
}

// Close marks the facade closed without closing the application-owned client.
func (store *RedisStore) Close() error    { store.closed.Store(true); return nil }
func (store *RedisStore) Keys() RedisKeys { return store.keys }

// LastDurabilityReport returns a copy of the most recent startup or health
// durability inspection.
func (store *RedisStore) LastDurabilityReport() RedisDurabilityReport {
	report := store.durability.Load()
	if report == nil {
		return RedisDurabilityReport{}
	}
	copy := *report
	copy.Warnings = append([]string(nil), report.Warnings...)
	return copy
}

func (store *RedisStore) ensure(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if store.closed.Load() {
		return ErrClosed
	}
	return nil
}

type wireRecord struct {
	EncodingVersion          int               `json:"encoding_version"`
	ID                       ID                `json:"id"`
	Destination              string            `json:"destination"`
	MessageType              string            `json:"message_type"`
	AggregateType            string            `json:"aggregate_type"`
	AggregateID              string            `json:"aggregate_id"`
	OrderingKey              string            `json:"ordering_key"`
	IdempotencyKey           string            `json:"idempotency_key"`
	Headers                  map[string]string `json:"headers"`
	Payload                  []byte            `json:"payload"`
	ContentDigest            string            `json:"content_digest"`
	State                    State             `json:"state"`
	Attempts                 int               `json:"attempts"`
	MaxAttempts              int               `json:"max_attempts"`
	AvailableAtUS            int64             `json:"available_at_us,string"`
	LeaseOwner               string            `json:"lease_owner"`
	LeaseToken               string            `json:"lease_token"`
	LeaseUntilUS             int64             `json:"lease_until_us,string"`
	LastErrorCode            string            `json:"last_error_code"`
	LastErrorMessage         string            `json:"last_error_message"`
	CreatedAtUS              int64             `json:"created_at_us,string"`
	UpdatedAtUS              int64             `json:"updated_at_us,string"`
	DeliveredAtUS            int64             `json:"delivered_at_us,string"`
	DeadAtUS                 int64             `json:"dead_at_us,string"`
	CancelledAtUS            int64             `json:"cancelled_at_us,string"`
	Version                  uint64            `json:"version"`
	QueryMember              string            `json:"query_member"`
	CompletedOwner           string            `json:"completed_owner"`
	CompletedToken           string            `json:"completed_token"`
	CompletedVersion         uint64            `json:"completed_version"`
	DestinationEncoded       string            `json:"destination_encoded"`
	PendingDestinationMember string            `json:"pending_destination_member"`
	DestinationQueryMember   string            `json:"destination_query_member"`
}

func wireFromRecord(record Record) wireRecord {
	wire := wireRecord{
		EncodingVersion: 1, ID: record.ID, Destination: record.Destination,
		MessageType: record.MessageType, AggregateType: record.AggregateType,
		AggregateID: record.AggregateID, OrderingKey: record.OrderingKey,
		IdempotencyKey: record.IdempotencyKey, Headers: record.Headers,
		Payload: record.Payload, ContentDigest: record.ContentDigest, State: record.State,
		Attempts: record.Attempts, MaxAttempts: record.MaxAttempts,
		AvailableAtUS: record.AvailableAt.UnixMicro(), LeaseOwner: record.LeaseOwner,
		LeaseToken: record.LeaseToken, LeaseUntilUS: pointerMicros(record.LeaseUntil),
		LastErrorCode: record.LastErrorCode, LastErrorMessage: record.LastErrorMessage,
		CreatedAtUS: record.CreatedAt.UnixMicro(), UpdatedAtUS: record.UpdatedAt.UnixMicro(),
		DeliveredAtUS: pointerMicros(record.DeliveredAt), DeadAtUS: pointerMicros(record.DeadAt),
		CancelledAtUS: pointerMicros(record.CancelledAt), Version: record.Version,
		QueryMember:        queryMember(record.CreatedAt.UnixMicro(), record.ID),
		DestinationEncoded: base64.RawURLEncoding.EncodeToString([]byte(record.Destination)),
	}
	wire.PendingDestinationMember = destinationMember(wire.DestinationEncoded, record.AvailableAt.UnixMicro(), record.ID)
	wire.DestinationQueryMember = destinationMember(wire.DestinationEncoded, record.CreatedAt.UnixMicro(), record.ID)
	return wire
}

func (wire wireRecord) record() (Record, error) {
	if wire.EncodingVersion != 1 {
		return Record{}, fmt.Errorf("unsupported Redis outbox encoding version %d", wire.EncodingVersion)
	}
	record := Record{
		ID: wire.ID, Destination: wire.Destination, MessageType: wire.MessageType,
		AggregateType: wire.AggregateType, AggregateID: wire.AggregateID,
		OrderingKey: wire.OrderingKey, IdempotencyKey: wire.IdempotencyKey,
		Headers: wire.Headers, Payload: wire.Payload, ContentDigest: wire.ContentDigest,
		State: wire.State, Attempts: wire.Attempts, MaxAttempts: wire.MaxAttempts,
		AvailableAt: time.UnixMicro(wire.AvailableAtUS).UTC(), LeaseOwner: wire.LeaseOwner,
		LeaseToken: wire.LeaseToken, LeaseUntil: microsPointer(wire.LeaseUntilUS),
		LastErrorCode: wire.LastErrorCode, LastErrorMessage: wire.LastErrorMessage,
		CreatedAt: time.UnixMicro(wire.CreatedAtUS).UTC(), UpdatedAt: time.UnixMicro(wire.UpdatedAtUS).UTC(),
		DeliveredAt: microsPointer(wire.DeliveredAtUS), DeadAt: microsPointer(wire.DeadAtUS),
		CancelledAt: microsPointer(wire.CancelledAtUS), Version: wire.Version,
	}
	return record.Clone(), nil
}

func encodeWire(wire wireRecord) (string, error) {
	encoded, err := json.Marshal(wire)
	if err != nil {
		return "", fmt.Errorf("encode Redis outbox record: %w", err)
	}
	return string(encoded), nil
}

func decodeWire(encoded string) (wireRecord, error) {
	var wire wireRecord
	if err := json.Unmarshal([]byte(encoded), &wire); err != nil {
		return wire, fmt.Errorf("decode Redis outbox record: %w", err)
	}
	if wire.EncodingVersion != 1 {
		return wire, fmt.Errorf("unsupported Redis outbox encoding version %d", wire.EncodingVersion)
	}
	return wire, nil
}

func decodeRecord(encoded string) (Record, error) {
	wire, err := decodeWire(encoded)
	if err != nil {
		return Record{}, err
	}
	return wire.record()
}

func pointerMicros(value *time.Time) int64 {
	if value == nil {
		return 0
	}
	return CanonicalTime(*value).UnixMicro()
}
func microsPointer(value int64) *time.Time {
	if value == 0 {
		return nil
	}
	result := time.UnixMicro(value).UTC()
	return &result
}
func queryMember(micros int64, id ID) string { return fmt.Sprintf("%020d|%s", micros, id) }
func destinationMember(encodedDestination string, micros int64, id ID) string {
	return fmt.Sprintf("%s|%020d|%s", encodedDestination, micros, id)
}
func idFromQueryMember(member string) (ID, error) {
	separator := strings.IndexByte(member, '|')
	if separator < 0 || separator == len(member)-1 {
		return "", fmt.Errorf("invalid Redis outbox query member")
	}
	return ID(member[separator+1:]), nil
}

func redisTime(ctx context.Context, client goredis.UniversalClient) (time.Time, error) {
	value, err := client.Time(ctx).Result()
	if err != nil {
		return time.Time{}, err
	}
	value = CanonicalTime(value)
	if err = ValidateTimestamp("storage_time", value); err != nil {
		return time.Time{}, err
	}
	return value, nil
}

func (store *RedisStore) runScript(ctx context.Context, script luaScript, keys []string, args ...any) (any, error) {
	result, err := store.client.EvalSha(ctx, script.script.Hash(), keys, args...).Result()
	if err != nil && strings.HasPrefix(err.Error(), "NOSCRIPT") {
		result, err = store.client.Eval(ctx, script.source, keys, args...).Result()
	}
	return result, err
}

func resultArray(value any) ([]any, error) {
	values, ok := value.([]any)
	if !ok {
		return nil, fmt.Errorf("unexpected Redis outbox script result %T", value)
	}
	return values, nil
}

func resultCode(value any) (int64, error) {
	switch code := value.(type) {
	case int64:
		return code, nil
	case string:
		var parsed int64
		_, err := fmt.Sscan(code, &parsed)
		return parsed, err
	default:
		return 0, fmt.Errorf("unexpected Redis outbox result code %T", value)
	}
}

func mapMutationCode(code int64) error {
	switch code {
	case 0, 1:
		return nil
	case -1:
		return ErrNotFound
	case -2:
		return ErrLeaseLost
	case -3:
		return ErrConflict
	case -4:
		return ErrInvalidTransition
	default:
		return fmt.Errorf("unexpected Redis outbox result code %d", code)
	}
}

func (store *RedisStore) Health(ctx context.Context) Health {
	if err := store.ensure(ctx); err != nil {
		return Health{Ready: false, StorageAvailable: false, Message: err.Error()}
	}
	if err := store.client.Ping(ctx).Err(); err != nil {
		return Health{Ready: false, StorageAvailable: false, Message: err.Error()}
	}
	reportValue, durabilityErr := store.CheckDurability(ctx)
	report := &reportValue
	safe := report.AOFEnabled && report.AOFLastWriteOK && report.EvictionPolicy == "noeviction"
	backlog, err := store.Backlog(ctx)
	if err != nil {
		return Health{Ready: false, StorageAvailable: false, DurabilitySafe: safe, Message: err.Error()}
	}
	ready := durabilityErr == nil
	return Health{Ready: ready, StorageAvailable: true, DurabilitySafe: safe, Backlog: backlog, Message: func() string {
		if durabilityErr != nil {
			return durabilityErr.Error()
		}
		return ""
	}()}
}

func isNil(err error) bool { return errors.Is(err, goredis.Nil) }
