package redis

import (
	"errors"
	"testing"

	"github.com/0x626f/react/outbox"
)

func TestKeysShareClusterSlot(t *testing.T) {
	keys, err := NewKeys("orders-test")
	if err != nil {
		t.Fatal(err)
	}
	all := append(keys.ScriptKeys(), keys.RecordKey("record/one"))
	want := ClusterSlot(all[0])
	for _, key := range all[1:] {
		if got := ClusterSlot(key); got != want {
			t.Fatalf("ClusterSlot(%q) = %d, want %d", key, got, want)
		}
	}
}

func TestKeysRejectHashTagEscape(t *testing.T) {
	for _, namespace := range []string{"", "orders}", "{orders}", "orders:other", "orders space"} {
		if namespace == "" {
			continue
		}
		if _, err := NewKeys(namespace); !errors.Is(err, outbox.ErrInvalidArgument) {
			t.Errorf("NewKeys(%q) error = %v, want ErrInvalidArgument", namespace, err)
		}
	}
}

func TestScriptVersions(t *testing.T) {
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

func TestMutationResultMapping(t *testing.T) {
	checks := map[int64]error{-1: outbox.ErrNotFound, -2: outbox.ErrLeaseLost, -3: outbox.ErrConflict, -4: outbox.ErrInvalidTransition}
	for code, want := range checks {
		if err := mapMutationCode(code); !errors.Is(err, want) {
			t.Errorf("mapMutationCode(%d) = %v, want %v", code, err, want)
		}
	}
}

func TestConfigKeepsEvictionOptOutDeliberate(t *testing.T) {
	config := DefaultConfig()
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

func TestConfigBoundsLuaData(t *testing.T) {
	config := DefaultConfig()
	config.MaxAppendEncodedBytes = 64 << 20
	config.MaxClaimResponseBytes = 64 << 20
	if _, err := config.normalized(); !errors.Is(err, outbox.ErrInvalidArgument) {
		t.Fatalf("unserviceable Lua byte limits error = %v, want ErrInvalidArgument", err)
	}
}

func TestClusterSlotUsesOnlyTheFirstHashTag(t *testing.T) {
	key := "prefix:{other}:react:outbox:{orders}:record"
	if tag := redisHashTag(key); tag != "other" {
		t.Fatalf("redisHashTag(%q) = %q, want other", key, tag)
	}
	if ClusterSlot(key) != ClusterSlot("{other}") {
		t.Fatal("ClusterSlot did not follow Redis first-hash-tag semantics")
	}
}
