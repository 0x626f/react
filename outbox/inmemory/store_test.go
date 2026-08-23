package inmemory_test

import (
	"testing"
	"time"

	"github.com/0x626f/react/outbox"
	"github.com/0x626f/react/outbox/inmemory"
)

func TestStoreContract(t *testing.T) {
	outbox.RunStoreContract(t, func(t testing.TB) outbox.TestHarness {
		clock := outbox.NewTestManualClock(time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC))
		ids := outbox.NewTestSequenceGenerator("generated-id")
		tokens := outbox.NewTestSequenceGenerator("lease-token")
		config := inmemory.DefaultConfig()
		config.Clock = clock
		config.IDGenerator = ids
		config.TokenGenerator = tokens
		store, err := inmemory.NewStore(config)
		if err != nil {
			t.Fatalf("NewStore: %v", err)
		}
		t.Cleanup(func() { _ = store.Close() })
		return outbox.TestHarness{
			Store:        store,
			Time:         outbox.TestManualTimeDriver{Clock: clock},
			Capabilities: outbox.TestCapabilities{AllQueryCombinations: true, Parallel: true},
		}
	})
}

func TestClosed(t *testing.T) {
	store, err := inmemory.NewStore(inmemory.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	if err = store.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err = store.Get(t.Context(), "anything"); err == nil {
		t.Fatal("Get after Close succeeded")
	}
}
