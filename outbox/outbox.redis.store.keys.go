package outbox

import (
	"encoding/base64"
	"fmt"
	"regexp"
)

var redisNamespacePattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$`)

// RedisKeys centralizes every Redis key. A namespace is also the Redis Cluster hash
// tag, so every atomic transition remains in exactly one hash slot.
type RedisKeys struct{ namespace, base string }

func NewRedisKeys(namespace string) (RedisKeys, error) {
	if !redisNamespacePattern.MatchString(namespace) {
		return RedisKeys{}, fmt.Errorf("%w: Redis namespace must contain only alphanumerics, '.', '_', or '-'", ErrInvalidArgument)
	}
	return RedisKeys{namespace: namespace, base: "react:outbox:{" + namespace + "}"}, nil
}

func (keys RedisKeys) Namespace() string           { return keys.namespace }
func (keys RedisKeys) Records() string             { return keys.base + ":records" }
func (keys RedisKeys) Idempotency() string         { return keys.base + ":idempotency" }
func (keys RedisKeys) Pending() string             { return keys.base + ":pending" }
func (keys RedisKeys) Leased() string              { return keys.base + ":leased" }
func (keys RedisKeys) Delivered() string           { return keys.base + ":delivered" }
func (keys RedisKeys) Dead() string                { return keys.base + ":dead" }
func (keys RedisKeys) Cancelled() string           { return keys.base + ":cancelled" }
func (keys RedisKeys) QueryAll() string            { return keys.base + ":query:all" }
func (keys RedisKeys) QueryPending() string        { return keys.base + ":query:pending" }
func (keys RedisKeys) QueryLeased() string         { return keys.base + ":query:leased" }
func (keys RedisKeys) QueryDelivered() string      { return keys.base + ":query:delivered" }
func (keys RedisKeys) QueryDead() string           { return keys.base + ":query:dead" }
func (keys RedisKeys) QueryCancelled() string      { return keys.base + ":query:cancelled" }
func (keys RedisKeys) PendingDestinations() string { return keys.base + ":pending:destinations" }
func (keys RedisKeys) QueryDestinations() string   { return keys.base + ":query:destinations" }

// RecordKey exposes the collision-safe conventional per-record key for domain
// composition tooling. The first-party representation keeps record blobs in a
// shared hash so Lua scripts access only explicitly declared keys.
func (keys RedisKeys) RecordKey(id ID) string {
	return keys.base + ":record:" + base64.RawURLEncoding.EncodeToString([]byte(id))
}

func (keys RedisKeys) State(state State) (string, error) {
	switch state {
	case StatePending:
		return keys.Pending(), nil
	case StateLeased:
		return keys.Leased(), nil
	case StateDelivered:
		return keys.Delivered(), nil
	case StateDead:
		return keys.Dead(), nil
	case StateCancelled:
		return keys.Cancelled(), nil
	default:
		return "", fmt.Errorf("%w: state", ErrInvalidArgument)
	}
}

func (keys RedisKeys) QueryState(state State) (string, error) {
	switch state {
	case StatePending:
		return keys.QueryPending(), nil
	case StateLeased:
		return keys.QueryLeased(), nil
	case StateDelivered:
		return keys.QueryDelivered(), nil
	case StateDead:
		return keys.QueryDead(), nil
	case StateCancelled:
		return keys.QueryCancelled(), nil
	default:
		return "", fmt.Errorf("%w: state", ErrInvalidArgument)
	}
}

func (keys RedisKeys) ScriptKeys() []string {
	return []string{
		keys.Records(), keys.Idempotency(), keys.Pending(), keys.Leased(),
		keys.Delivered(), keys.Dead(), keys.Cancelled(), keys.QueryAll(),
		keys.QueryPending(), keys.QueryLeased(), keys.QueryDelivered(),
		keys.QueryDead(), keys.QueryCancelled(), keys.PendingDestinations(),
		keys.QueryDestinations(),
	}
}

// RedisClusterSlot returns the Redis Cluster slot for a key and is useful for
// deployment validation and third-party composition tests.
func RedisClusterSlot(key string) uint16 {
	data := []byte(key)
	if tag := redisHashTag(key); tag != "" {
		data = []byte(tag)
	}
	return crc16(data) % 16384
}

func redisHashTag(key string) string {
	start := -1
	for index, value := range []byte(key) {
		if value == '{' {
			start = index + 1
			break
		}
	}
	if start < 0 {
		return ""
	}
	data := []byte(key)
	for index := start; index < len(data); index++ {
		if data[index] == '}' {
			if index == start {
				return ""
			}
			return string(data[start:index])
		}
	}
	return ""
}

func crc16(data []byte) uint16 {
	var crc uint16
	for _, value := range data {
		crc ^= uint16(value) << 8
		for range 8 {
			if crc&0x8000 != 0 {
				crc = (crc << 1) ^ 0x1021
			} else {
				crc <<= 1
			}
		}
	}
	return crc
}
