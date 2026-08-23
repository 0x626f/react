package outbox

import (
	"errors"
	"testing"
)

func TestRedisKeysShareClusterSlot(t *testing.T) {
	keys, err := NewRedisKeys("orders-test")
	if err != nil {
		t.Fatal(err)
	}
	all := append(keys.ScriptKeys(), keys.RecordKey("record/one"))
	want := RedisClusterSlot(all[0])
	for _, key := range all[1:] {
		if got := RedisClusterSlot(key); got != want {
			t.Fatalf("RedisClusterSlot(%q) = %d, want %d", key, got, want)
		}
	}
}

func TestRedisKeysRejectHashTagEscape(t *testing.T) {
	for _, namespace := range []string{"", "orders}", "{orders}", "orders:other", "orders space"} {
		if namespace == "" {
			continue
		}
		if _, err := NewRedisKeys(namespace); !errors.Is(err, ErrInvalidArgument) {
			t.Errorf("NewRedisKeys(%q) error = %v, want ErrInvalidArgument", namespace, err)
		}
	}
}

func TestRedisScriptVersions(t *testing.T) {
	for name, script := range map[string]luaScript{
		"append": appendScript, "claim": claimScript, "renew": renewScript,
		"acknowledge": acknowledgeScript, "retry": retryScript,
		"release": releaseScript, "dead_letter": deadLetterScript,
		"cancel": cancelScript, "reschedule": rescheduleScript,
		"requeue": requeueScript, "purge": purgeScript,
	} {
		if len(script.source) < len("-- react-outbox:v1:") || script.source[:len("-- react-outbox:v1:")] != "-- react-outbox:v1:" {
			t.Errorf("%s script has no version header", name)
		}
	}
}

func TestRedisMutationResultMapping(t *testing.T) {
	checks := map[int64]error{-1: ErrNotFound, -2: ErrLeaseLost, -3: ErrConflict, -4: ErrInvalidTransition}
	for code, want := range checks {
		if err := mapMutationCode(code); !errors.Is(err, want) {
			t.Errorf("mapMutationCode(%d) = %v, want %v", code, err, want)
		}
	}
}

func TestRedisConfigKeepsEvictionOptOutDeliberate(t *testing.T) {
	config := DefaultRedisConfig()
	config.RequireNoEviction = false
	config.AllowUnsafeEviction = false
	normalized, err := config.normalized()
	if err != nil {
		t.Fatal(err)
	}
	if !normalized.RequireNoEviction {
		t.Fatal("zero-value eviction settings disabled the noeviction requirement")
	}
	config.AllowUnsafeEviction = true
	normalized, err = config.normalized()
	if err != nil {
		t.Fatal(err)
	}
	if normalized.RequireNoEviction {
		t.Fatal("explicit unsafe eviction opt-out was ignored")
	}
}

func TestRedisConfigBoundsLuaData(t *testing.T) {
	config := DefaultRedisConfig()
	config.MaxAppendEncodedBytes = 64 << 20
	config.MaxClaimResponseBytes = 64 << 20
	if _, err := config.normalized(); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("unserviceable Lua byte limits error = %v, want ErrInvalidArgument", err)
	}
}

func TestRedisClusterSlotUsesOnlyTheFirstHashTag(t *testing.T) {
	key := "prefix:{other}:react:outbox:{orders}:record"
	if tag := redisHashTag(key); tag != "other" {
		t.Fatalf("redisHashTag(%q) = %q, want other", key, tag)
	}
	if RedisClusterSlot(key) != RedisClusterSlot("{other}") {
		t.Fatal("RedisClusterSlot did not follow Redis first-hash-tag semantics")
	}
}
