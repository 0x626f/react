package outbox_test

import (
	"testing"
	"time"

	"github.com/0x626f/react/outbox"
)

func TestInmemoryStoreContract(t *testing.T) {
	outbox.RunStoreContract(t, func(t testing.TB) outbox.TestHarness {
		clock := outbox.NewTestManualClock(time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC))
		ids := outbox.NewTestSequenceGenerator("generated-id")
		tokens := outbox.NewTestSequenceGenerator("lease-token")
		config := outbox.DefaultInmemoryConfig()
		config.Clock = clock
		config.IDGenerator = ids
		config.TokenGenerator = tokens
		store, err := outbox.NewInmemoryStore(config)
		if err != nil {
			t.Fatalf("NewInmemoryStore: %v", err)
		}
		t.Cleanup(func() { _ = store.Close() })
		return outbox.TestHarness{
			Store:        store,
			Time:         outbox.TestManualTimeDriver{Clock: clock},
			Capabilities: outbox.TestCapabilities{AllQueryCombinations: true, Parallel: true},
		}
	})
}

func TestInmemoryClosed(t *testing.T) {
	store, err := outbox.NewInmemoryStore(outbox.DefaultInmemoryConfig())
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
