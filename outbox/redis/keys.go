package redis

import (
	"encoding/base64"
	"fmt"
	"regexp"

	"github.com/0x626f/react/outbox"
)

var namespacePattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$`)

// Keys centralizes every Redis key. A namespace is also the Redis Cluster hash
// tag, so every atomic transition remains in exactly one hash slot.
type Keys struct{ namespace, base string }

func NewKeys(namespace string) (Keys, error) {
	if !namespacePattern.MatchString(namespace) {
		return Keys{}, fmt.Errorf("%w: Redis namespace must contain only alphanumerics, '.', '_', or '-'", outbox.ErrInvalidArgument)
	}
	return Keys{namespace: namespace, base: "react:outbox:{" + namespace + "}"}, nil
}

func (keys Keys) Namespace() string           { return keys.namespace }
func (keys Keys) Records() string             { return keys.base + ":records" }
func (keys Keys) Idempotency() string         { return keys.base + ":idempotency" }
func (keys Keys) Pending() string             { return keys.base + ":pending" }
func (keys Keys) Leased() string              { return keys.base + ":leased" }
func (keys Keys) Delivered() string           { return keys.base + ":delivered" }
func (keys Keys) Dead() string                { return keys.base + ":dead" }
func (keys Keys) Cancelled() string           { return keys.base + ":cancelled" }
func (keys Keys) QueryAll() string            { return keys.base + ":query:all" }
func (keys Keys) QueryPending() string        { return keys.base + ":query:pending" }
func (keys Keys) QueryLeased() string         { return keys.base + ":query:leased" }
func (keys Keys) QueryDelivered() string      { return keys.base + ":query:delivered" }
func (keys Keys) QueryDead() string           { return keys.base + ":query:dead" }
func (keys Keys) QueryCancelled() string      { return keys.base + ":query:cancelled" }
func (keys Keys) PendingDestinations() string { return keys.base + ":pending:destinations" }
func (keys Keys) QueryDestinations() string   { return keys.base + ":query:destinations" }

// RecordKey exposes the collision-safe conventional per-record key for domain
// composition tooling. The first-party representation keeps record blobs in a
// shared hash so Lua scripts access only explicitly declared keys.
func (keys Keys) RecordKey(id outbox.ID) string {
	return keys.base + ":record:" + base64.RawURLEncoding.EncodeToString([]byte(id))
}

func (keys Keys) State(state outbox.State) (string, error) {
	switch state {
	case outbox.StatePending:
		return keys.Pending(), nil
	case outbox.StateLeased:
		return keys.Leased(), nil
	case outbox.StateDelivered:
		return keys.Delivered(), nil
	case outbox.StateDead:
		return keys.Dead(), nil
	case outbox.StateCancelled:
		return keys.Cancelled(), nil
	default:
		return "", fmt.Errorf("%w: state", outbox.ErrInvalidArgument)
	}
}

func (keys Keys) QueryState(state outbox.State) (string, error) {
	switch state {
	case outbox.StatePending:
		return keys.QueryPending(), nil
	case outbox.StateLeased:
		return keys.QueryLeased(), nil
	case outbox.StateDelivered:
		return keys.QueryDelivered(), nil
	case outbox.StateDead:
		return keys.QueryDead(), nil
	case outbox.StateCancelled:
		return keys.QueryCancelled(), nil
	default:
		return "", fmt.Errorf("%w: state", outbox.ErrInvalidArgument)
	}
}

func (keys Keys) ScriptKeys() []string {
	return []string{
		keys.Records(), keys.Idempotency(), keys.Pending(), keys.Leased(),
		keys.Delivered(), keys.Dead(), keys.Cancelled(), keys.QueryAll(),
		keys.QueryPending(), keys.QueryLeased(), keys.QueryDelivered(),
		keys.QueryDead(), keys.QueryCancelled(), keys.PendingDestinations(),
		keys.QueryDestinations(),
	}
}

// ClusterSlot returns the Redis Cluster slot for a key and is useful for
// deployment validation and third-party composition tests.
func ClusterSlot(key string) uint16 {
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
