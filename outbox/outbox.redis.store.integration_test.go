package outbox

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

const redisTestURLVariable = "OUTBOX_REDIS_TEST_URL"

func TestRedisStoreContract(t *testing.T) {
	url := os.Getenv(redisTestURLVariable)
	if url == "" {
		t.Skipf("set %s to run Redis integration tests", redisTestURLVariable)
	}
	runStoreContract(t, func(t testing.TB) testHarness {
		client, store := newRedisIntegrationStore(t, url)
		t.Cleanup(func() { cleanupRedisNamespace(t, client, store.Keys()); _ = store.Close(); _ = client.Close() })
		return testHarness{
			Store: store,
			Time: testWallTimeDriver{NowFunc: func(ctx context.Context) (time.Time, error) {
				value, err := client.Time(ctx).Result()
				return CanonicalTime(value), err
			}},
			Capabilities: testCapabilities{
				UnsupportedQuery: &Query{States: []State{StatePending}, Destinations: []string{"events"}},
			},
		}
	})
}

func TestRedisScriptCacheReloadAndNoActiveTTL(t *testing.T) {
	url := os.Getenv(redisTestURLVariable)
	if url == "" {
		t.Skipf("set %s to run Redis integration tests", redisTestURLVariable)
	}
	client, store := newRedisIntegrationStore(t, url)
	t.Cleanup(func() { cleanupRedisNamespace(t, client, store.Keys()); _ = store.Close(); _ = client.Close() })
	ctx := t.Context()
	if _, err := store.Append(ctx, testRecord(testWithID("script-reload"))); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{store.Keys().Records(), store.Keys().Pending()} {
		ttl, err := client.TTL(ctx, key).Result()
		if err != nil {
			t.Fatal(err)
		}
		if ttl != -1 {
			t.Fatalf("active key %q TTL = %v, want no TTL", key, ttl)
		}
	}
	if os.Getenv("OUTBOX_REDIS_ALLOW_SCRIPT_FLUSH") == "1" {
		if err := client.ScriptFlush(ctx).Err(); err != nil {
			t.Fatal(err)
		}
	} else {
		t.Log("set OUTBOX_REDIS_ALLOW_SCRIPT_FLUSH=1 to additionally exercise NOSCRIPT recovery on an isolated Redis")
	}
	if _, err := store.Claim(ctx, ClaimRequest{Owner: "reload-worker", Limit: 1, LeaseDuration: time.Second}); err != nil {
		t.Fatalf("Claim after SCRIPT FLUSH: %v", err)
	}
	if ttl, err := client.TTL(ctx, store.Keys().Leased()).Result(); err != nil || ttl != -1 {
		t.Fatalf("leased key TTL = %v, %v; want no TTL", ttl, err)
	}
}

func TestRedisDestinationFilteredClaim(t *testing.T) {
	url := os.Getenv(redisTestURLVariable)
	if url == "" {
		t.Skipf("set %s to run Redis integration tests", redisTestURLVariable)
	}
	client, store := newRedisIntegrationStore(t, url)
	t.Cleanup(func() { cleanupRedisNamespace(t, client, store.Keys()); _ = store.Close(); _ = client.Close() })
	ctx := t.Context()
	if _, err := store.Append(ctx,
		testRecord(testWithID("destination-a"), testWithDestination("alpha")),
		testRecord(testWithID("destination-b"), testWithDestination("beta")),
	); err != nil {
		t.Fatal(err)
	}
	records, err := store.Claim(ctx, ClaimRequest{Owner: "filtered", Limit: 2, LeaseDuration: time.Second, Destinations: []string{"beta"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 || records[0].ID != "destination-b" {
		t.Fatalf("filtered Claim = %#v", records)
	}
}

func TestRedisPaginationRemainsFiniteAcrossConcurrentAppend(t *testing.T) {
	url := os.Getenv(redisTestURLVariable)
	if url == "" {
		t.Skipf("set %s to run Redis integration tests", redisTestURLVariable)
	}
	client, store := newRedisIntegrationStore(t, url)
	t.Cleanup(func() { cleanupRedisNamespace(t, client, store.Keys()); _ = store.Close(); _ = client.Close() })
	ctx := t.Context()
	if _, err := store.Append(ctx,
		testRecord(testWithID("cursor-a")),
		testRecord(testWithID("cursor-b")),
		testRecord(testWithID("cursor-c")),
	); err != nil {
		t.Fatal(err)
	}
	page, err := store.Find(ctx, Query{Sort: SortCreatedAt, Direction: SortAscending, Limit: 2})
	if err != nil || len(page.Records) != 2 || page.NextCursor == "" {
		t.Fatalf("first page = %#v, %v", page, err)
	}
	if _, err = store.Append(ctx, testRecord(testWithID("cursor-d"))); err != nil {
		t.Fatal(err)
	}
	seen := map[ID]struct{}{page.Records[0].ID: {}, page.Records[1].ID: {}}
	cursor := page.NextCursor
	for pages := 0; cursor != "" && pages < 10; pages++ {
		page, err = store.Find(ctx, Query{Sort: SortCreatedAt, Direction: SortAscending, Limit: 2, Cursor: cursor})
		if err != nil {
			t.Fatal(err)
		}
		for _, record := range page.Records {
			if _, duplicate := seen[record.ID]; duplicate {
				t.Fatalf("record %q appeared on more than one page", record.ID)
			}
			seen[record.ID] = struct{}{}
		}
		cursor = page.NextCursor
	}
	if cursor != "" || len(seen) != 4 {
		t.Fatalf("pagination did not terminate with all records: cursor=%q seen=%v", cursor, seen)
	}
}

func TestRedisScriptTypeValidationPrecedesWrites(t *testing.T) {
	url := os.Getenv(redisTestURLVariable)
	if url == "" {
		t.Skipf("set %s to run Redis integration tests", redisTestURLVariable)
	}
	client, store := newRedisIntegrationStore(t, url)
	t.Cleanup(func() { cleanupRedisNamespace(t, client, store.Keys()); _ = store.Close(); _ = client.Close() })
	ctx := t.Context()
	if err := client.Set(ctx, store.Keys().Pending(), "wrong-type", 0).Err(); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(ctx, testRecord(testWithID("wrong-type-append"))); err == nil {
		t.Fatal("Append with a wrong-type index succeeded")
	}
	if _, err := store.Get(ctx, "wrong-type-append"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("failed script partially inserted a record: %v", err)
	}
}

func TestRedisDurabilityReport(t *testing.T) {
	url := os.Getenv(redisTestURLVariable)
	if url == "" {
		t.Skipf("set %s to run Redis integration tests", redisTestURLVariable)
	}
	client, store := newRedisIntegrationStore(t, url)
	t.Cleanup(func() { cleanupRedisNamespace(t, client, store.Keys()); _ = store.Close(); _ = client.Close() })
	report, err := store.CheckDurability(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if !report.Checked {
		t.Fatal("durability report was not checked")
	}
}

func TestRedisConnectionLossAndContextCancellation(t *testing.T) {
	url := os.Getenv(redisTestURLVariable)
	if url == "" {
		t.Skipf("set %s to run Redis integration tests", redisTestURLVariable)
	}
	client, store := newRedisIntegrationStore(t, url)
	t.Cleanup(func() { _ = store.Close(); _ = client.Close() })
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := store.Get(ctx, "cancelled-request"); !errors.Is(err, context.Canceled) {
		t.Fatalf("Get with cancelled context = %v, want context.Canceled", err)
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Get(context.Background(), "lost-connection"); err == nil || errors.Is(err, ErrNotFound) {
		t.Fatalf("Get after connection loss = %v, want a client connection error", err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestRedisAtomicRedisDomainComposition(t *testing.T) {
	url := os.Getenv(redisTestURLVariable)
	if url == "" {
		t.Skipf("set %s to run Redis integration tests", redisTestURLVariable)
	}
	client, store := newRedisIntegrationStore(t, url)
	t.Cleanup(func() { cleanupRedisNamespace(t, client, store.Keys()); _ = store.Close(); _ = client.Close() })
	domainKey := "react:outbox:{" + store.Keys().Namespace() + "}:domain:order-1"
	t.Cleanup(func() { _ = client.Del(context.Background(), domainKey).Err() })
	validation := `
local domain_type = redis.call('TYPE', KEYS[DOMAIN_KEY_OFFSET+1])['ok']
if domain_type ~= 'none' and domain_type ~= 'string' then return redis.error_reply('DOMAIN_WRONGTYPE') end
local domain_current = redis.call('GET', KEYS[DOMAIN_KEY_OFFSET+1])
if domain_current and domain_current ~= ARGV[DOMAIN_ARG_OFFSET+1] then return {-3} end
`
	apply := `redis.call('SET', KEYS[DOMAIN_KEY_OFFSET+1], ARGV[DOMAIN_ARG_OFFSET+2])`
	request := RedisCompositionRequest{
		Append:     AppendRequest{Records: []NewRecord{testRecord(testWithID("composed-record"))}, DuplicateMode: RejectDuplicate},
		DomainKeys: []string{domainKey}, DomainArguments: []any{"", "confirmed"},
		ValidateLua: validation, ApplyLua: apply,
	}
	if _, err := store.Compose(t.Context(), request); err != nil {
		t.Fatal(err)
	}
	if value, err := client.Get(t.Context(), domainKey).Result(); err != nil || value != "confirmed" {
		t.Fatalf("domain value = %q, %v", value, err)
	}

	request.Append.Records[0] = testRecord(testWithID("rejected-composed-record"))
	request.DomainArguments = []any{"unexpected", "changed"}
	if _, err := store.Compose(t.Context(), request); !errors.Is(err, ErrConflict) {
		t.Fatalf("conflicting Compose = %v", err)
	}
	if _, err := store.Get(t.Context(), "rejected-composed-record"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("outbox side of rejected composition = %v", err)
	}
	if value, _ := client.Get(t.Context(), domainKey).Result(); value != "confirmed" {
		t.Fatalf("domain changed after rejected composition: %q", value)
	}
}

func newRedisIntegrationStore(t testing.TB, rawURL string) (goredis.UniversalClient, *RedisStore) {
	t.Helper()
	options, err := goredis.ParseURL(rawURL)
	if err != nil {
		t.Fatalf("redis.ParseURL: %v", err)
	}
	client := goredis.NewClient(options)
	namespace := fmt.Sprintf("test-%d", time.Now().UnixNano())
	config := DefaultRedisConfig()
	config.Namespace = namespace
	config.RequireNoEviction = false
	config.AllowUnsafeEviction = true
	store, err := NewRedisStore(context.Background(), client, config)
	if err != nil {
		_ = client.Close()
		t.Fatalf("NewRedisStore: %v", err)
	}
	return client, store
}

func cleanupRedisNamespace(t testing.TB, client goredis.UniversalClient, keys RedisKeys) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	// Exact, namespace-scoped keys only. Never FLUSHALL or scan a user instance.
	if err := client.Del(ctx, keys.ScriptKeys()...).Err(); err != nil {
		t.Errorf("Redis namespace cleanup: %v", err)
	}
}
