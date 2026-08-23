package outbox

// Package-local conformance helpers live only in the test build. Production
// structures and services do not expose or depend on them.

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"
)

// ITestStore is the aggregate portable contract exercised by runStoreContract.
// It exists only in the package's test build.
type ITestStore interface {
	IAppender
	IDeliveryStore
	IReader
	IMaintenanceStore
}

// ITestTimeDriver exposes authoritative adapter time and bounded advancement.
// It exists only in the package's test build.
type ITestTimeDriver interface {
	Now(ctx context.Context) (time.Time, error)
	Elapse(ctx context.Context, duration time.Duration) error
}

// testCapabilities declares genuine optional adapter behavior. It never
// disables required state, fencing, append, or portable query checks.
type testCapabilities struct {
	// AllQueryCombinations enables checks beyond the required portable query
	// subset (ID, state, created-time, deterministic created ordering).
	AllQueryCombinations bool
	// UnsupportedQuery is run when non-zero and must return
	// ErrUnsupportedCriteria.
	UnsupportedQuery              *Query
	SameResourceDomainComposition bool
	Parallel                      bool
}

// testHarness is one isolated conformance-test instance.
type testHarness struct {
	Store        ITestStore
	Time         ITestTimeDriver
	Capabilities testCapabilities
}

// testStoreFactory constructs a fresh harness and registers cleanup on t.
type testStoreFactory func(t testing.TB) testHarness

// runStoreContract runs the portable first-party storage contract. The factory
// is called inside every subtest and must return a completely isolated store
// and register all cleanup with t.Cleanup.
//
// A third-party adapter normally invokes it from an external test package:
//
//	func TestStoreContract(t *testing.T) {
//		outbox.runStoreContract(t, func(t testing.TB) outbox.testHarness {
//			store, timeDriver := newIsolatedStore(t)
//			return outbox.testHarness{Store: store, Time: timeDriver}
//		})
//	}
func runStoreContract(t *testing.T, factory testStoreFactory) {
	t.Helper()
	run := func(name string, test func(*testing.T, testHarness)) {
		t.Run(name, func(t *testing.T) {
			harness := factory(t)
			if harness.Store == nil {
				t.Fatal("factory returned a nil Store")
			}
			if harness.Time == nil {
				t.Fatal("factory returned a nil Time driver")
			}
			if harness.Capabilities.Parallel {
				t.Parallel()
			}
			test(t, harness)
		})
	}

	run("validation_and_atomic_batch", testContractValidationAndAtomicBatch)
	run("duplicates", testContractDuplicates)
	run("copy_isolation", testContractCopyIsolation)
	run("pagination", testContractPagination)
	run("query_capabilities", testContractQueryCapabilities)
	run("claim_acknowledge_and_fence", testContractClaimAcknowledge)
	run("retry_release_and_dead_letter", testContractSettlements)
	run("illegal_transitions_and_stale_fences", testContractIllegalTransitions)
	run("attempt_exhaustion", testContractAttemptExhaustion)
	run("lease_expiry_and_renewal", testContractLeaseExpiry)
	run("concurrent_claimers", testContractConcurrentClaimers)
	run("maintenance", testContractMaintenance)
	run("context_cancellation", testContractContextCancellation)
	run("unsupported_query", testContractUnsupportedQuery)
	run("failure_text_bounds", testContractFailureTextBounds)
	run("closed_store", testContractClosedStore)
}

func testContractValidationAndAtomicBatch(t *testing.T, harness testHarness) {
	ctx := context.Background()
	valid := testRecord(testWithID("valid-batch-record"))
	invalidRecord := testRecord(testWithID("invalid-batch-record"))
	invalidRecord.Destination = ""
	if _, err := harness.Store.Append(ctx, valid, invalidRecord); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("Append invalid batch error = %v, want ErrInvalidArgument", err)
	}
	if _, err := harness.Store.Get(ctx, valid.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("valid member of rejected batch exists or Get error = %v", err)
	}
	if _, err := harness.Store.Append(ctx, testRecord(testWithID("valid-record"))); err != nil {
		t.Fatalf("Append valid record: %v", err)
	}
	invalidUTF8 := testRecord(testWithID("invalid-utf8"))
	invalidUTF8.MessageType = string([]byte{0xff})
	if _, err := harness.Store.Append(ctx, invalidUTF8); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("Append invalid UTF-8 error = %v, want ErrInvalidArgument", err)
	}
	invalidHeader := testRecord(testWithID("invalid-header"), testWithHeader("secret", "value\x00suffix"))
	if _, err := harness.Store.Append(ctx, invalidHeader); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("Append NUL header error = %v, want ErrInvalidArgument", err)
	}
	tooLate := testRecord(testWithID("invalid-time"), testWithAvailableAt(time.UnixMicro(MaximumTimestampUnixMicro).Add(time.Microsecond)))
	if _, err := harness.Store.Append(ctx, tooLate); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("Append out-of-range timestamp error = %v, want ErrInvalidArgument", err)
	}
	if _, err := harness.Store.Claim(ctx, ClaimRequest{Owner: "precision-worker", Limit: 1, LeaseDuration: time.Nanosecond}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("Claim sub-microsecond lease error = %v, want ErrInvalidArgument", err)
	}
}

func testContractDuplicates(t *testing.T, harness testHarness) {
	ctx := context.Background()
	record := testRecord(testWithID("duplicate-record"), testWithIdempotencyKey("duplicate-key"))
	first, err := harness.Store.Append(ctx, record)
	if err != nil {
		t.Fatalf("first Append: %v", err)
	}
	if _, err = harness.Store.Append(ctx, record); !errors.Is(err, ErrDuplicateID) {
		t.Fatalf("duplicate Append error = %v, want ErrDuplicateID", err)
	}
	batchAppender, ok := harness.Store.(IBatchAppender)
	if !ok {
		t.Fatal("first-party testStore must implement IBatchAppender")
	}
	identical, err := batchAppender.AppendBatch(ctx, AppendRequest{Records: []NewRecord{record}, DuplicateMode: AcceptIdentical})
	if err != nil {
		t.Fatalf("AcceptIdentical AppendBatch: %v", err)
	}
	if len(identical) != 1 || identical[0].ID != first[0].ID {
		t.Fatalf("identical duplicate = %#v, want existing %q", identical, first[0].ID)
	}
	conflict := record.Clone()
	conflict.Payload = []byte("different")
	if _, err = batchAppender.AppendBatch(ctx, AppendRequest{Records: []NewRecord{conflict}, DuplicateMode: AcceptIdentical}); !errors.Is(err, ErrConflict) {
		t.Fatalf("conflicting duplicate error = %v, want ErrConflict", err)
	}
	atomicNew := testRecord(testWithID("must-not-partially-append"))
	if _, err = batchAppender.AppendBatch(ctx, AppendRequest{Records: []NewRecord{atomicNew, conflict}, DuplicateMode: AcceptIdentical}); !errors.Is(err, ErrConflict) {
		t.Fatalf("atomic conflicting batch error = %v, want ErrConflict", err)
	}
	if _, err = harness.Store.Get(ctx, atomicNew.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("conflicting batch inserted an earlier member: %v", err)
	}

	const callers = 8
	results := make(chan error, callers)
	for range callers {
		go func() {
			_, appendErr := batchAppender.AppendBatch(ctx, AppendRequest{Records: []NewRecord{testRecord(testWithID("concurrent-duplicate"))}, DuplicateMode: AcceptIdentical})
			results <- appendErr
		}()
	}
	for range callers {
		if appendErr := <-results; appendErr != nil {
			t.Errorf("concurrent identical append: %v", appendErr)
		}
	}

	start := make(chan struct{})
	conflictingResults := make(chan error, 2)
	for _, payload := range [][]byte{[]byte("concurrent-a"), []byte("concurrent-b")} {
		payload := payload
		go func() {
			<-start
			_, appendErr := batchAppender.AppendBatch(ctx, AppendRequest{Records: []NewRecord{testRecord(testWithID("concurrent-conflict"), testWithPayload(payload))}, DuplicateMode: AcceptIdentical})
			conflictingResults <- appendErr
		}()
	}
	close(start)
	var successes, conflicts int
	for range 2 {
		switch err := <-conflictingResults; {
		case err == nil:
			successes++
		case errors.Is(err, ErrConflict):
			conflicts++
		default:
			t.Fatalf("concurrent conflicting append error = %v", err)
		}
	}
	if successes != 1 || conflicts != 1 {
		t.Fatalf("concurrent conflicting results: successes=%d conflicts=%d", successes, conflicts)
	}
}

func testContractCopyIsolation(t *testing.T, harness testHarness) {
	ctx := context.Background()
	input := testRecord(testWithID("copy-record"), testWithPayload([]byte("original")), testWithHeader("trace", "one"))
	created, err := harness.Store.Append(ctx, input)
	if err != nil {
		t.Fatal(err)
	}
	input.Payload[0] = 'X'
	input.Headers["trace"] = "changed"
	created[0].Payload[0] = 'Y'
	created[0].Headers["trace"] = "changed-again"
	stored, err := harness.Store.Get(ctx, "copy-record")
	if err != nil {
		t.Fatal(err)
	}
	if string(stored.Payload) != "original" || stored.Headers["trace"] != "one" {
		t.Fatalf("stored immutable content was aliased: %#v", stored)
	}
	stored.Payload[0] = 'Z'
	stored.Headers["trace"] = "third-change"
	again, err := harness.Store.Get(ctx, "copy-record")
	if err != nil {
		t.Fatal(err)
	}
	if string(again.Payload) != "original" || again.Headers["trace"] != "one" {
		t.Fatal("Get returned aliased content")
	}
}

func testContractPagination(t *testing.T, harness testHarness) {
	ctx := context.Background()
	now, err := harness.Time.Now(ctx)
	if err != nil {
		t.Fatal(err)
	}
	inputs := []NewRecord{
		testRecord(testWithID("page-c"), testWithAvailableAt(now)),
		testRecord(testWithID("page-a"), testWithAvailableAt(now)),
		testRecord(testWithID("page-b"), testWithAvailableAt(now)),
	}
	if _, err = harness.Store.Append(ctx, inputs...); err != nil {
		t.Fatal(err)
	}
	first, err := harness.Store.Find(ctx, Query{States: []State{StatePending}, Sort: SortCreatedAt, Direction: SortAscending, Limit: 2})
	if err != nil {
		t.Fatal(err)
	}
	if len(first.Records) != 2 || first.NextCursor == "" {
		t.Fatalf("first page = %#v", first)
	}
	second, err := harness.Store.Find(ctx, Query{States: []State{StatePending}, Sort: SortCreatedAt, Direction: SortAscending, Limit: 2, Cursor: first.NextCursor})
	if err != nil {
		t.Fatal(err)
	}
	if len(second.Records) != 1 {
		t.Fatalf("second page count = %d, want 1", len(second.Records))
	}
	seen := map[ID]bool{}
	for _, record := range append(first.Records, second.Records...) {
		if seen[record.ID] {
			t.Fatalf("duplicate paginated ID %q", record.ID)
		}
		seen[record.ID] = true
	}
	if len(seen) != 3 {
		t.Fatalf("paginated IDs = %v", seen)
	}
	if first.Records[0].ID != "page-a" || first.Records[1].ID != "page-b" || second.Records[0].ID != "page-c" {
		t.Fatalf("ascending stable tie order = %q, %q, %q", first.Records[0].ID, first.Records[1].ID, second.Records[0].ID)
	}
	descending, err := harness.Store.Find(ctx, Query{Destinations: []string{"events"}, Sort: SortCreatedAt, Direction: SortDescending, Limit: 2})
	if err != nil {
		t.Fatal(err)
	}
	if len(descending.Records) != 2 || descending.Records[0].ID != "page-c" || descending.Records[1].ID != "page-b" || descending.NextCursor == "" {
		t.Fatalf("descending first page = %#v", descending)
	}
	descendingTail, err := harness.Store.Find(ctx, Query{Destinations: []string{"events"}, Sort: SortCreatedAt, Direction: SortDescending, Limit: 2, Cursor: descending.NextCursor})
	if err != nil || len(descendingTail.Records) != 1 || descendingTail.Records[0].ID != "page-a" {
		t.Fatalf("descending tail = %#v, %v", descendingTail, err)
	}
	if _, err = harness.Store.Find(ctx, Query{Sort: SortAvailableAt, Cursor: first.NextCursor}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("mismatched cursor error = %v, want ErrInvalidArgument", err)
	}
}

func testContractQueryCapabilities(t *testing.T, harness testHarness) {
	ctx := context.Background()
	now, err := harness.Time.Now(ctx)
	if err != nil {
		t.Fatal(err)
	}
	created, err := harness.Store.Append(ctx,
		testRecord(testWithID("query-alpha"), testWithDestination("alpha"), testWithMessageType("alpha.created"), testWithAggregate("order", "one"), testWithOrderingKey("order-one"), testWithIdempotencyKey("query-alpha-key"), testWithAvailableAt(now.Add(time.Hour))),
		testRecord(testWithID("query-beta"), testWithDestination("beta"), testWithMessageType("beta.created"), testWithAggregate("order", "two"), testWithOrderingKey("order-two"), testWithIdempotencyKey("query-beta-key"), testWithAvailableAt(now.Add(2*time.Hour))),
	)
	if err != nil {
		t.Fatal(err)
	}
	page, err := harness.Store.Find(ctx, Query{IDs: []ID{"query-alpha", "query-beta"}, States: []State{StatePending}, Destinations: []string{"alpha"}, Limit: 10})
	if err != nil || len(page.Records) != 1 || page.Records[0].ID != "query-alpha" {
		t.Fatalf("bounded ID query = %#v, %v", page, err)
	}
	page, err = harness.Store.Find(ctx, Query{Destinations: []string{"beta"}, Limit: 10})
	if err != nil || len(page.Records) != 1 || page.Records[0].ID != "query-beta" {
		t.Fatalf("destination query = %#v, %v", page, err)
	}
	from, to := created[0].CreatedAt, created[1].CreatedAt
	if from.After(to) {
		from, to = to, from
	}
	page, err = harness.Store.Find(ctx, Query{CreatedAt: TimeRange{From: &from, To: &to}, Limit: 10})
	if err != nil || len(page.Records) != 2 {
		t.Fatalf("created range query = %#v, %v", page, err)
	}
	if !harness.Capabilities.AllQueryCombinations {
		return
	}
	page, err = harness.Store.Find(ctx, Query{
		MessageTypes: []string{"alpha.created"}, AggregateType: "order", AggregateID: "one",
		OrderingKey: "order-one", IdempotencyKey: "query-alpha-key", Sort: SortAvailableAt, Limit: 10,
	})
	if err != nil || len(page.Records) != 1 || page.Records[0].ID != "query-alpha" {
		t.Fatalf("fully indexed query = %#v, %v", page, err)
	}
}

func testContractClaimAcknowledge(t *testing.T, harness testHarness) {
	ctx := context.Background()
	if _, err := harness.Store.Append(ctx, testRecord(testWithID("ack-record"))); err != nil {
		t.Fatal(err)
	}
	claimed := testClaimOne(t, harness, "worker-a", time.Minute)
	if claimed.State != StateLeased || claimed.Attempts != 1 || claimed.LeaseToken == "" || claimed.Version < 2 {
		t.Fatalf("claimed record = %#v", claimed)
	}
	stale := claimed.LeaseRef()
	stale.Token += "-stale"
	if err := harness.Store.Acknowledge(ctx, stale, DeliveryResult{}); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("stale ack = %v, want ErrLeaseLost", err)
	}
	lease := claimed.LeaseRef()
	if err := harness.Store.Acknowledge(ctx, lease, DeliveryResult{}); err != nil {
		t.Fatalf("Acknowledge: %v", err)
	}
	// Implementations may prove this exact completed fence and return nil.
	if err := harness.Store.Acknowledge(ctx, lease, DeliveryResult{}); err != nil && !errors.Is(err, ErrConflict) {
		t.Fatalf("second Acknowledge = %v, want nil or ErrConflict", err)
	}
	differentCompletion := lease
	differentCompletion.Token += "-different"
	if err := harness.Store.Acknowledge(ctx, differentCompletion, DeliveryResult{}); !errors.Is(err, ErrConflict) {
		t.Fatalf("different completed Acknowledge = %v, want ErrConflict", err)
	}
	delivered := testRequireState(t, harness.Store, claimed.ID, StateDelivered)
	if delivered.DeliveredAt == nil || delivered.LeaseToken != "" {
		t.Fatalf("delivered record = %#v", delivered)
	}
	if err := harness.Store.Cancel(ctx, delivered.ID, "late"); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("Cancel delivered = %v", err)
	}
}

func testContractSettlements(t *testing.T, harness testHarness) {
	ctx := context.Background()
	now, err := harness.Time.Now(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = harness.Store.Append(ctx,
		testRecord(testWithID("retry-record"), testWithMaxAttempts(3)),
		testRecord(testWithID("release-record"), testWithMaxAttempts(3)),
		testRecord(testWithID("dead-record"), testWithMaxAttempts(3)),
	); err != nil {
		t.Fatal(err)
	}

	claimed, err := harness.Store.Claim(ctx, ClaimRequest{Owner: "worker", Limit: 3, LeaseDuration: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	byID := make(map[ID]Record, len(claimed))
	for _, record := range claimed {
		byID[record.ID] = record
	}
	if err = harness.Store.Retry(ctx, byID["retry-record"].LeaseRef(), RetryRequest{AvailableAt: now.Add(time.Second), Failure: Failure{Code: "temporary", Message: "retry me"}}); err != nil {
		t.Fatal(err)
	}
	if err = harness.Store.Release(ctx, byID["release-record"].LeaseRef(), now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if err = harness.Store.DeadLetter(ctx, byID["dead-record"].LeaseRef(), Failure{Code: "terminal", Message: "do not retry"}); err != nil {
		t.Fatal(err)
	}
	testRequireState(t, harness.Store, "retry-record", StatePending)
	testRequireState(t, harness.Store, "release-record", StatePending)
	dead := testRequireState(t, harness.Store, "dead-record", StateDead)
	if dead.DeadAt == nil || dead.LastErrorCode != "terminal" {
		t.Fatalf("dead record = %#v", dead)
	}
}

func testContractIllegalTransitions(t *testing.T, harness testHarness) {
	ctx := context.Background()
	now, err := harness.Time.Now(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := harness.Store.Append(ctx,
		testRecord(testWithID("illegal-pending")),
		testRecord(testWithID("illegal-leased")),
		testRecord(testWithID("illegal-dead")),
	); err != nil {
		t.Fatal(err)
	}

	fake := LeaseRef{ID: "illegal-pending", Owner: "worker", Token: "stale-token", Version: 1}
	for operation, err := range map[string]error{
		"renew":       harness.Store.Renew(ctx, fake, now.Add(time.Minute)),
		"acknowledge": harness.Store.Acknowledge(ctx, fake, DeliveryResult{}),
		"retry":       harness.Store.Retry(ctx, fake, RetryRequest{AvailableAt: now, Failure: Failure{Code: "stale"}}),
		"release":     harness.Store.Release(ctx, fake, now),
		"dead_letter": harness.Store.DeadLetter(ctx, fake, Failure{Code: "stale"}),
	} {
		if !errors.Is(err, ErrLeaseLost) {
			t.Errorf("%s pending record error = %v, want ErrLeaseLost", operation, err)
		}
	}

	claimed, err := harness.Store.Claim(ctx, ClaimRequest{Owner: "transition-worker", Limit: 2, LeaseDuration: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	byID := make(map[ID]Record, len(claimed))
	for _, record := range claimed {
		byID[record.ID] = record
	}
	leased := byID["illegal-leased"]
	deadLease := byID["illegal-dead"]
	if leased.ID == "" || deadLease.ID == "" {
		t.Fatalf("expected leased records, got %#v", claimed)
	}
	if err = harness.Store.Cancel(ctx, leased.ID, "not pending"); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("Cancel leased = %v, want ErrInvalidTransition", err)
	}
	if err = harness.Store.Reschedule(ctx, leased.ID, now.Add(time.Hour)); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("Reschedule leased = %v, want ErrInvalidTransition", err)
	}
	if err = harness.Store.Requeue(ctx, leased.ID, RequeueOptions{ResetAttempts: true}); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("Requeue leased = %v, want ErrInvalidTransition", err)
	}
	stale := leased.LeaseRef()
	stale.Version++
	for operation, settleErr := range map[string]error{
		"renew":       harness.Store.Renew(ctx, stale, now.Add(time.Minute)),
		"acknowledge": harness.Store.Acknowledge(ctx, stale, DeliveryResult{}),
		"retry":       harness.Store.Retry(ctx, stale, RetryRequest{AvailableAt: now, Failure: Failure{Code: "stale"}}),
		"release":     harness.Store.Release(ctx, stale, now),
		"dead_letter": harness.Store.DeadLetter(ctx, stale, Failure{Code: "stale"}),
	} {
		if !errors.Is(settleErr, ErrLeaseLost) {
			t.Errorf("%s stale version error = %v, want ErrLeaseLost", operation, settleErr)
		}
	}
	if err = harness.Store.Acknowledge(ctx, leased.LeaseRef(), DeliveryResult{}); err != nil {
		t.Fatal(err)
	}
	if err = harness.Store.DeadLetter(ctx, deadLease.LeaseRef(), Failure{Code: "terminal"}); err != nil {
		t.Fatal(err)
	}
	for _, id := range []ID{leased.ID, deadLease.ID} {
		if err = harness.Store.Cancel(ctx, id, "terminal"); !errors.Is(err, ErrInvalidTransition) {
			t.Errorf("Cancel terminal %q = %v, want ErrInvalidTransition", id, err)
		}
		if err = harness.Store.Reschedule(ctx, id, now); !errors.Is(err, ErrInvalidTransition) {
			t.Errorf("Reschedule terminal %q = %v, want ErrInvalidTransition", id, err)
		}
	}
	if err = harness.Store.Requeue(ctx, leased.ID, RequeueOptions{ResetAttempts: true}); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("Requeue delivered = %v, want ErrInvalidTransition", err)
	}
}

func testContractAttemptExhaustion(t *testing.T, harness testHarness) {
	ctx := context.Background()
	now, err := harness.Time.Now(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = harness.Store.Append(ctx,
		testRecord(testWithID("retry-exhausted"), testWithMaxAttempts(1)),
		testRecord(testWithID("release-exhausted"), testWithMaxAttempts(1)),
	); err != nil {
		t.Fatal(err)
	}
	claimed, err := harness.Store.Claim(ctx, ClaimRequest{Owner: "exhaustion-worker", Limit: 2, LeaseDuration: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	byID := make(map[ID]Record, len(claimed))
	for _, record := range claimed {
		byID[record.ID] = record
	}
	if err = harness.Store.Retry(ctx, byID["retry-exhausted"].LeaseRef(), RetryRequest{AvailableAt: now, Failure: Failure{Code: "temporary"}}); err != nil {
		t.Fatal(err)
	}
	if err = harness.Store.Release(ctx, byID["release-exhausted"].LeaseRef(), now); err != nil {
		t.Fatal(err)
	}
	for _, id := range []ID{"retry-exhausted", "release-exhausted"} {
		record := testRequireState(t, harness.Store, id, StateDead)
		if record.Attempts != 1 {
			t.Errorf("%q attempts = %d, want 1", id, record.Attempts)
		}
		if err = harness.Store.Requeue(ctx, id, RequeueOptions{}); !errors.Is(err, ErrInvalidArgument) {
			t.Errorf("Requeue exhausted %q preserving attempts = %v, want ErrInvalidArgument", id, err)
		}
		if err = harness.Store.Requeue(ctx, id, RequeueOptions{ResetAttempts: true}); err != nil {
			t.Errorf("Requeue exhausted %q with reset: %v", id, err)
		}
	}
	if _, err = harness.Store.Append(ctx, testRecord(testWithID("expiry-exhausted"), testWithDestination("expiry-only"), testWithMaxAttempts(1))); err != nil {
		t.Fatal(err)
	}
	expiring, err := harness.Store.Claim(ctx, ClaimRequest{Owner: "expiry-worker", Limit: 1, LeaseDuration: 50 * time.Millisecond, Destinations: []string{"expiry-only"}})
	if err != nil || len(expiring) != 1 {
		t.Fatalf("claim expiry-exhausted = %#v, %v", expiring, err)
	}
	if err = harness.Time.Elapse(ctx, 100*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	recovered, err := harness.Store.Claim(ctx, ClaimRequest{Owner: "recovery-worker", Limit: 1, LeaseDuration: time.Second, Destinations: []string{"expiry-only"}})
	if err != nil || len(recovered) != 0 {
		t.Fatalf("exhausted expiry recovery claimed %#v, %v", recovered, err)
	}
	testRequireState(t, harness.Store, "expiry-exhausted", StateDead)
}

func testContractLeaseExpiry(t *testing.T, harness testHarness) {
	ctx := context.Background()
	if _, err := harness.Store.Append(ctx, testRecord(testWithID("lease-record"), testWithMaxAttempts(3))); err != nil {
		t.Fatal(err)
	}
	claimed := testClaimOne(t, harness, "old-worker", 100*time.Millisecond)
	now, err := harness.Time.Now(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err = harness.Store.Renew(ctx, claimed.LeaseRef(), now.Add(250*time.Millisecond)); err != nil {
		t.Fatalf("Renew: %v", err)
	}
	if err = harness.Time.Elapse(ctx, 150*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if err = harness.Store.Acknowledge(ctx, claimed.LeaseRef(), DeliveryResult{}); err != nil {
		t.Fatalf("acknowledge renewed lease: %v", err)
	}

	if _, err = harness.Store.Append(ctx, testRecord(testWithID("expired-record"), testWithMaxAttempts(3))); err != nil {
		t.Fatal(err)
	}
	expired := testClaimOne(t, harness, "stale-worker", 50*time.Millisecond)
	if err = harness.Time.Elapse(ctx, 100*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if err = harness.Store.Acknowledge(ctx, expired.LeaseRef(), DeliveryResult{}); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("expired Acknowledge = %v", err)
	}
	reclaimed := testClaimOne(t, harness, "new-worker", 100*time.Millisecond)
	if reclaimed.ID != expired.ID || reclaimed.LeaseToken == expired.LeaseToken || reclaimed.Version == expired.Version {
		t.Fatalf("reclaimed record did not receive a fresh fence: old=%#v new=%#v", expired, reclaimed)
	}
}

func testContractConcurrentClaimers(t *testing.T, harness testHarness) {
	ctx := context.Background()
	const count = 20
	inputs := make([]NewRecord, 0, count)
	for index := range count {
		inputs = append(inputs, testRecord(testWithID(ID(fmt.Sprintf("claim-%02d", index)))))
	}
	if _, err := harness.Store.Append(ctx, inputs...); err != nil {
		t.Fatal(err)
	}
	results := make(chan []Record, 4)
	errorsChannel := make(chan error, 4)
	for worker := range 4 {
		go func(worker int) {
			records, err := harness.Store.Claim(ctx, ClaimRequest{Owner: fmt.Sprintf("worker-%d", worker), Limit: count, LeaseDuration: time.Minute})
			results <- records
			errorsChannel <- err
		}(worker)
	}
	seen := make(map[ID]string)
	for range 4 {
		for _, record := range <-results {
			if owner, exists := seen[record.ID]; exists {
				t.Errorf("record %q claimed by %q and %q", record.ID, owner, record.LeaseOwner)
			}
			seen[record.ID] = record.LeaseOwner
		}
		if err := <-errorsChannel; err != nil {
			t.Errorf("Claim: %v", err)
		}
	}
	if len(seen) != count {
		t.Fatalf("claimed %d unique records, want %d", len(seen), count)
	}
}

func testContractMaintenance(t *testing.T, harness testHarness) {
	ctx := context.Background()
	now, err := harness.Time.Now(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = harness.Store.Append(ctx,
		testRecord(testWithID("cancel-record")),
		testRecord(testWithID("cancel-record-two")),
		testRecord(testWithID("reschedule-record")),
		testRecord(testWithID("requeue-record")),
	); err != nil {
		t.Fatal(err)
	}
	if err = harness.Store.Cancel(ctx, "cancel-record", "operator request"); err != nil {
		t.Fatal(err)
	}
	if err = harness.Store.Cancel(ctx, "cancel-record", "operator request"); err != nil {
		t.Fatalf("idempotent Cancel: %v", err)
	}
	if err = harness.Store.Cancel(ctx, "cancel-record-two", "operator request"); err != nil {
		t.Fatal(err)
	}
	if err = harness.Store.Reschedule(ctx, "cancel-record", now); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("Reschedule cancelled = %v, want ErrInvalidTransition", err)
	}
	if err = harness.Store.Requeue(ctx, "cancel-record", RequeueOptions{ResetAttempts: true}); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("Requeue cancelled = %v, want ErrInvalidTransition", err)
	}
	if _, err = harness.Store.Purge(ctx, PurgeRequest{States: []State{StatePending}, Before: now.Add(time.Hour), Limit: 1}); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("Purge pending = %v, want ErrInvalidTransition", err)
	}
	if _, err = harness.Store.Purge(ctx, PurgeRequest{States: []State{StateCancelled, StateCancelled, StateCancelled, StateCancelled}, Before: now.Add(time.Hour), Limit: 1}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("Purge unbounded state list = %v, want ErrInvalidArgument", err)
	}
	if err = harness.Store.Reschedule(ctx, "reschedule-record", now.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	rescheduled := testRequireState(t, harness.Store, "reschedule-record", StatePending)
	if !rescheduled.AvailableAt.Equal(CanonicalTime(now.Add(time.Hour))) {
		t.Fatalf("AvailableAt = %v", rescheduled.AvailableAt)
	}
	claimed, err := harness.Store.Claim(ctx, ClaimRequest{Owner: "maintenance", Limit: 10, LeaseDuration: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	var requeueLease LeaseRef
	for _, record := range claimed {
		if record.ID == "requeue-record" {
			requeueLease = record.LeaseRef()
		}
	}
	if requeueLease.ID == "" {
		t.Fatal("requeue record was not claimed")
	}
	if err = harness.Store.DeadLetter(ctx, requeueLease, Failure{Code: "bad"}); err != nil {
		t.Fatal(err)
	}
	if err = harness.Store.Requeue(ctx, "requeue-record", RequeueOptions{AvailableAt: now, ResetAttempts: true}); err != nil {
		t.Fatal(err)
	}
	requeued := testRequireState(t, harness.Store, "requeue-record", StatePending)
	if requeued.Attempts != 0 || requeued.LastErrorCode != "" {
		t.Fatalf("requeued record = %#v", requeued)
	}
	if err = harness.Time.Elapse(ctx, 10*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	after, err := harness.Time.Now(ctx)
	if err != nil {
		t.Fatal(err)
	}
	purged, err := harness.Store.Purge(ctx, PurgeRequest{States: []State{StateCancelled}, Before: after, Limit: 1})
	if err != nil || purged != 1 {
		t.Fatalf("Purge = %d, %v", purged, err)
	}
	remaining, err := harness.Store.Find(ctx, Query{IDs: []ID{"cancel-record", "cancel-record-two"}, Limit: 10})
	if err != nil || len(remaining.Records) != 1 {
		t.Fatalf("bounded Purge left %#v, %v; want one record", remaining, err)
	}
	purged, err = harness.Store.Purge(ctx, PurgeRequest{States: []State{StateCancelled}, Before: after, Limit: 1})
	if err != nil || purged != 1 {
		t.Fatalf("retry Purge = %d, %v", purged, err)
	}
	for _, id := range []ID{"cancel-record", "cancel-record-two"} {
		if _, err = harness.Store.Get(ctx, id); !errors.Is(err, ErrNotFound) {
			t.Fatalf("purged Get(%q) = %v", id, err)
		}
	}
}

func testContractContextCancellation(t *testing.T, harness testHarness) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := harness.Store.Append(ctx, testRecord(testWithID("cancelled-context"))); !errors.Is(err, context.Canceled) {
		t.Fatalf("Append cancelled context = %v", err)
	}
	if _, err := harness.Store.Find(ctx, Query{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("Find cancelled context = %v", err)
	}
}

func testContractUnsupportedQuery(t *testing.T, harness testHarness) {
	if harness.Capabilities.UnsupportedQuery == nil {
		return
	}
	if _, err := harness.Store.Find(context.Background(), *harness.Capabilities.UnsupportedQuery); !errors.Is(err, ErrUnsupportedCriteria) {
		t.Fatalf("unsupported Find error = %v, want ErrUnsupportedCriteria", err)
	}
}

func testContractFailureTextBounds(t *testing.T, harness testHarness) {
	ctx := context.Background()
	if _, err := harness.Store.Append(ctx, testRecord(testWithID("bounded-failure"))); err != nil {
		t.Fatal(err)
	}
	claimed := testClaimOne(t, harness, "failure-worker", time.Minute)
	invalid := string([]byte{'b', 'a', 'd', 0, 0xff})
	failure := Failure{Code: invalid + strings.Repeat("c", DefaultLimits().MaxErrorCodeBytes+50), Message: invalid + strings.Repeat("m", DefaultLimits().MaxErrorMessageBytes+50)}
	if err := harness.Store.DeadLetter(ctx, claimed.LeaseRef(), failure); err != nil {
		t.Fatal(err)
	}
	dead := testRequireState(t, harness.Store, claimed.ID, StateDead)
	if len(dead.LastErrorCode) > DefaultLimits().MaxErrorCodeBytes || len(dead.LastErrorMessage) > DefaultLimits().MaxErrorMessageBytes {
		t.Fatalf("failure was not bounded: code=%d message=%d", len(dead.LastErrorCode), len(dead.LastErrorMessage))
	}
	if !utf8.ValidString(dead.LastErrorCode) || !utf8.ValidString(dead.LastErrorMessage) || strings.ContainsRune(dead.LastErrorCode, 0) || strings.ContainsRune(dead.LastErrorMessage, 0) {
		t.Fatalf("failure was not made storage-safe: code=%q message=%q", dead.LastErrorCode, dead.LastErrorMessage)
	}
}

func testContractClosedStore(t *testing.T, harness testHarness) {
	closer, ok := harness.Store.(interface{ Close() error })
	if !ok {
		return
	}
	if err := closer.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := closer.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	ctx := context.Background()
	if _, err := harness.Store.Append(ctx, testRecord(testWithID("after-close"))); !errors.Is(err, ErrClosed) {
		t.Fatalf("Append after Close = %v, want ErrClosed", err)
	}
	if _, err := harness.Store.Append(ctx); !errors.Is(err, ErrClosed) {
		t.Fatalf("empty Append after Close = %v, want ErrClosed", err)
	}
	if _, err := harness.Store.Get(ctx, "after-close"); !errors.Is(err, ErrClosed) {
		t.Fatalf("Get after Close = %v, want ErrClosed", err)
	}
	if _, err := harness.Store.Find(ctx, Query{}); !errors.Is(err, ErrClosed) {
		t.Fatalf("Find after Close = %v, want ErrClosed", err)
	}
}

func testClaimOne(t testing.TB, harness testHarness, owner string, duration time.Duration) Record {
	t.Helper()
	records, err := harness.Store.Claim(context.Background(), ClaimRequest{Owner: owner, Limit: 1, LeaseDuration: duration})
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("Claim returned %d records, want 1", len(records))
	}
	return records[0]
}

// testRecordOption customizes a record returned by testRecord.
type testRecordOption func(*NewRecord)

// testRecord builds a valid record with a unique default ID.
func testRecord(options ...testRecordOption) NewRecord {
	var random [12]byte
	if _, err := rand.Read(random[:]); err != nil {
		panic(fmt.Sprintf("generate test record ID: %v", err))
	}
	record := NewRecord{
		ID: ID("test-" + hex.EncodeToString(random[:])), Destination: "events",
		MessageType: "example.created", Headers: map[string]string{"content-type": "application/json"},
		Payload: []byte(`{"id":"example"}`), MaxAttempts: 3,
	}
	for _, option := range options {
		option(&record)
	}
	return record
}

// testWithID sets a test record ID.
func testWithID(id ID) testRecordOption { return func(record *NewRecord) { record.ID = id } }

// testWithDestination sets a test destination.
func testWithDestination(value string) testRecordOption {
	return func(record *NewRecord) { record.Destination = value }
}

// testWithMessageType sets a test message type.
func testWithMessageType(value string) testRecordOption {
	return func(record *NewRecord) { record.MessageType = value }
}

// testWithAggregate sets test aggregate metadata.
func testWithAggregate(kind, id string) testRecordOption {
	return func(record *NewRecord) { record.AggregateType, record.AggregateID = kind, id }
}

// testWithOrderingKey sets a test ordering key.
func testWithOrderingKey(value string) testRecordOption {
	return func(record *NewRecord) { record.OrderingKey = value }
}

// testWithIdempotencyKey sets a test idempotency key.
func testWithIdempotencyKey(value string) testRecordOption {
	return func(record *NewRecord) { record.IdempotencyKey = value }
}

// testWithPayload copies and sets a test payload.
func testWithPayload(value []byte) testRecordOption {
	return func(record *NewRecord) { record.Payload = bytes.Clone(value) }
}

// testWithHeader adds or replaces a test header.
func testWithHeader(key, value string) testRecordOption {
	return func(record *NewRecord) {
		if record.Headers == nil {
			record.Headers = map[string]string{}
		}
		record.Headers[key] = value
	}
}

// testWithAvailableAt sets test availability.
func testWithAvailableAt(value time.Time) testRecordOption {
	return func(record *NewRecord) { record.AvailableAt = value }
}

// testWithMaxAttempts sets the persisted test attempt limit.
func testWithMaxAttempts(value int) testRecordOption {
	return func(record *NewRecord) { record.MaxAttempts = value }
}

// testAssertImmutableEqual compares immutable content on the owning test goroutine.
func testAssertImmutableEqual(t testing.TB, want, got Record) {
	t.Helper()
	if want.ID != got.ID || want.Destination != got.Destination || want.MessageType != got.MessageType ||
		want.AggregateType != got.AggregateType || want.AggregateID != got.AggregateID ||
		want.OrderingKey != got.OrderingKey || want.IdempotencyKey != got.IdempotencyKey ||
		want.ContentDigest != got.ContentDigest || !bytes.Equal(want.Payload, got.Payload) || !equalHeaders(want.Headers, got.Headers) {
		t.Fatalf("immutable records differ\nwant: %#v\ngot:  %#v", want, got)
	}
}

func equalHeaders(left, right map[string]string) bool {
	if len(left) != len(right) {
		return false
	}
	for key, value := range left {
		if right[key] != value {
			return false
		}
	}
	return true
}

// testRequireState loads a record and asserts its durable state.
func testRequireState(t testing.TB, reader IReader, id ID, state State) Record {
	t.Helper()
	record, err := reader.Get(context.Background(), id)
	if err != nil {
		t.Fatalf("Get(%q): %v", id, err)
	}
	if record.State != state {
		t.Fatalf("record %q state = %q, want %q", id, record.State, state)
	}
	return record
}

// testAssertLease verifies the public shape of an active lease.
func testAssertLease(t testing.TB, record Record, owner string) {
	t.Helper()
	if record.State != StateLeased || record.LeaseOwner != owner || record.LeaseToken == "" || record.LeaseUntil == nil || record.Version == 0 {
		t.Fatalf("invalid lease: %#v", record)
	}
}

// testWallTimeDriver is suitable for integration stores whose authoritative
// server time cannot be advanced. Elapse uses a cancellable timer and includes
// a small precision allowance; it never performs an unconditional long sleep.
type testWallTimeDriver struct {
	NowFunc func(context.Context) (time.Time, error)
}

func (driver testWallTimeDriver) Now(ctx context.Context) (time.Time, error) {
	return driver.NowFunc(ctx)
}
func (driver testWallTimeDriver) Elapse(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration + 2*time.Millisecond)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// testRecordingSink records copied successful deliveries in call order.
type testRecordingSink struct {
	mu      sync.Mutex
	records []Record
}

func (sink *testRecordingSink) Deliver(ctx context.Context, record Record) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	sink.mu.Lock()
	sink.records = append(sink.records, record.Clone())
	sink.mu.Unlock()
	return nil
}

// Records returns copied calls in delivery order.
func (sink *testRecordingSink) Records() []Record {
	sink.mu.Lock()
	defer sink.mu.Unlock()
	result := make([]Record, len(sink.records))
	for index := range sink.records {
		result[index] = sink.records[index].Clone()
	}
	return result
}

// testScriptedSink records copied calls and returns a configured error sequence.
type testScriptedSink struct {
	mu     sync.Mutex
	errors []error
	calls  []Record
}

// newTestScriptedSink constructs a sink with successive outcomes.
func newTestScriptedSink(outcomes ...error) *testScriptedSink {
	return &testScriptedSink{errors: append([]error(nil), outcomes...)}
}
func (sink *testScriptedSink) Deliver(ctx context.Context, record Record) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	sink.mu.Lock()
	defer sink.mu.Unlock()
	sink.calls = append(sink.calls, record.Clone())
	if len(sink.errors) == 0 {
		return nil
	}
	err := sink.errors[0]
	sink.errors = sink.errors[1:]
	return err
}

// Calls returns copied calls in delivery order.
func (sink *testScriptedSink) Calls() []Record {
	sink.mu.Lock()
	defer sink.mu.Unlock()
	result := make([]Record, len(sink.calls))
	for index := range sink.calls {
		result[index] = sink.calls[index].Clone()
	}
	return result
}

// testBlockingSink reports starts and waits for Release or cancellation.
type testBlockingSink struct {
	Started chan Record
	Release <-chan struct{}
}

func (sink *testBlockingSink) Deliver(ctx context.Context, record Record) error {
	select {
	case sink.Started <- record.Clone():
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case <-sink.Release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Fault-injection operation names accepted by testFaultStore.Fail.
const (
	testOperationAppend      = "append"
	testOperationClaim       = "claim"
	testOperationRenew       = "renew"
	testOperationAcknowledge = "acknowledge"
	testOperationRetry       = "retry"
	testOperationDeadLetter  = "dead_letter"
	testOperationRelease     = "release"
	testOperationGet         = "get"
	testOperationFind        = "find"
	testOperationCancel      = "cancel"
	testOperationReschedule  = "reschedule"
	testOperationRequeue     = "requeue"
	testOperationPurge       = "purge"
)

// testFaultStore decorates a portable store with bounded named failures.
type testFaultStore struct {
	Store  ITestStore
	mu     sync.Mutex
	faults map[string]*testFault
}
type testFault struct {
	remaining int
	err       error
}

// newTestFaultStore constructs a fault decorator with no active faults.
func newTestFaultStore(store ITestStore) *testFaultStore {
	return &testFaultStore{Store: store, faults: make(map[string]*testFault)}
}

// Fail configures the next count calls of operation to return err.
func (store *testFaultStore) Fail(operation string, count int, err error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.faults[operation] = &testFault{remaining: count, err: err}
}
func (store *testFaultStore) failure(operation string) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	fault := store.faults[operation]
	if fault == nil || fault.remaining <= 0 {
		return nil
	}
	fault.remaining--
	return fault.err
}
func (store *testFaultStore) Append(ctx context.Context, records ...NewRecord) ([]Record, error) {
	if err := store.failure(testOperationAppend); err != nil {
		return nil, err
	}
	return store.Store.Append(ctx, records...)
}
func (store *testFaultStore) AppendBatch(ctx context.Context, request AppendRequest) ([]Record, error) {
	if err := store.failure(testOperationAppend); err != nil {
		return nil, err
	}
	appender, ok := store.Store.(IBatchAppender)
	if !ok {
		return nil, ErrUnsupportedCriteria
	}
	return appender.AppendBatch(ctx, request)
}
func (store *testFaultStore) Claim(ctx context.Context, request ClaimRequest) ([]Record, error) {
	if err := store.failure(testOperationClaim); err != nil {
		return nil, err
	}
	return store.Store.Claim(ctx, request)
}
func (store *testFaultStore) Renew(ctx context.Context, lease LeaseRef, until time.Time) error {
	if err := store.failure(testOperationRenew); err != nil {
		return err
	}
	return store.Store.Renew(ctx, lease, until)
}
func (store *testFaultStore) Acknowledge(ctx context.Context, lease LeaseRef, result DeliveryResult) error {
	if err := store.failure(testOperationAcknowledge); err != nil {
		return err
	}
	return store.Store.Acknowledge(ctx, lease, result)
}
func (store *testFaultStore) Retry(ctx context.Context, lease LeaseRef, request RetryRequest) error {
	if err := store.failure(testOperationRetry); err != nil {
		return err
	}
	return store.Store.Retry(ctx, lease, request)
}
func (store *testFaultStore) DeadLetter(ctx context.Context, lease LeaseRef, failure Failure) error {
	if err := store.failure(testOperationDeadLetter); err != nil {
		return err
	}
	return store.Store.DeadLetter(ctx, lease, failure)
}
func (store *testFaultStore) Release(ctx context.Context, lease LeaseRef, at time.Time) error {
	if err := store.failure(testOperationRelease); err != nil {
		return err
	}
	return store.Store.Release(ctx, lease, at)
}
func (store *testFaultStore) Get(ctx context.Context, id ID) (Record, error) {
	if err := store.failure(testOperationGet); err != nil {
		return Record{}, err
	}
	return store.Store.Get(ctx, id)
}
func (store *testFaultStore) Find(ctx context.Context, query Query) (Page, error) {
	if err := store.failure(testOperationFind); err != nil {
		return Page{}, err
	}
	return store.Store.Find(ctx, query)
}
func (store *testFaultStore) Cancel(ctx context.Context, id ID, reason string) error {
	if err := store.failure(testOperationCancel); err != nil {
		return err
	}
	return store.Store.Cancel(ctx, id, reason)
}
func (store *testFaultStore) Reschedule(ctx context.Context, id ID, at time.Time) error {
	if err := store.failure(testOperationReschedule); err != nil {
		return err
	}
	return store.Store.Reschedule(ctx, id, at)
}
func (store *testFaultStore) Requeue(ctx context.Context, id ID, options RequeueOptions) error {
	if err := store.failure(testOperationRequeue); err != nil {
		return err
	}
	return store.Store.Requeue(ctx, id, options)
}
func (store *testFaultStore) Purge(ctx context.Context, request PurgeRequest) (int, error) {
	if err := store.failure(testOperationPurge); err != nil {
		return 0, err
	}
	return store.Store.Purge(ctx, request)
}

// testWaitForState polls with a bounded context until a record reaches state.
func testWaitForState(ctx context.Context, reader IReader, id ID, state State, interval time.Duration) (Record, error) {
	if interval <= 0 {
		interval = time.Millisecond
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		record, err := reader.Get(ctx, id)
		if err == nil && record.State == state {
			return record, nil
		}
		if err != nil && !errors.Is(err, ErrNotFound) {
			return Record{}, err
		}
		select {
		case <-ctx.Done():
			return Record{}, ctx.Err()
		case <-ticker.C:
		}
	}
}

var _ IStore = (*testFaultStore)(nil)
