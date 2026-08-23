package outbox

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/0x626f/author"
	"github.com/0x626f/gioc"
	"github.com/0x626f/react"
)

func TestServiceSuccessfulDelivery(t *testing.T) {
	store := newServiceStore(t)
	record := testRecord(testWithID("delivery-success"))
	if _, err := store.Append(t.Context(), record); err != nil {
		t.Fatal(err)
	}
	sink := &testRecordingSink{}
	service := newTestService(t, store, sink, withOwner("success-worker"))

	delivered := waitState(t, store, record.ID, StateDelivered, nil)
	if delivered.Attempts != 1 || len(sink.Records()) != 1 {
		t.Fatalf("delivered=%#v calls=%d", delivered, len(sink.Records()))
	}
	shutdownService(t, service)
}

func TestServiceRetryAndTerminal(t *testing.T) {
	t.Run("retryable", func(t *testing.T) {
		store := newServiceStore(t)
		if _, err := store.Append(t.Context(), testRecord(testWithID("delivery-retry"))); err != nil {
			t.Fatal(err)
		}
		sink := newTestScriptedSink(errors.New("temporary"), nil)
		service := newTestService(t, store, sink,
			withOwner("retry-worker"),
			withRetryPolicy(testRetryPolicy{delay: 5 * time.Millisecond, retry: true}),
		)
		waitState(t, store, "delivery-retry", StatePending, func(record Record) bool { return record.Attempts == 1 })
		waitState(t, store, "delivery-retry", StateDelivered, nil)
		if calls := len(sink.Calls()); calls != 2 {
			t.Fatalf("sink calls = %d, want 2", calls)
		}
		shutdownService(t, service)
	})

	t.Run("terminal", func(t *testing.T) {
		store := newServiceStore(t)
		if _, err := store.Append(t.Context(), testRecord(testWithID("delivery-terminal"))); err != nil {
			t.Fatal(err)
		}
		sink := newTestScriptedSink(&TerminalError{Err: errors.New("invalid route")})
		service := newTestService(t, store, sink, withOwner("terminal-worker"))
		dead := waitState(t, store, "delivery-terminal", StateDead, nil)
		if dead.LastErrorCode != string(OutcomeTerminal) {
			t.Fatalf("failure code = %q", dead.LastErrorCode)
		}
		shutdownService(t, service)
	})
}

func TestServiceAckFailureLeavesAtLeastOnceDuplicateWindow(t *testing.T) {
	store := newServiceStore(t)
	if _, err := store.Append(t.Context(), testRecord(testWithID("duplicate-window"), testWithMaxAttempts(3))); err != nil {
		t.Fatal(err)
	}
	faults := newTestFaultStore(store)
	faults.Fail(testOperationAcknowledge, 1, errors.New("simulated crash after publish"))
	firstSink := &testRecordingSink{}
	first := newTestServiceWithLease(t, faults, firstSink, 20*time.Millisecond, withOwner("first-worker"))
	waitState(t, store, "duplicate-window", StateLeased, func(Record) bool { return len(firstSink.Records()) == 1 })
	shutdownService(t, first)

	time.Sleep(25 * time.Millisecond)
	secondSink := &testRecordingSink{}
	second := newTestServiceWithLease(t, store, secondSink, 20*time.Millisecond, withOwner("second-worker"))
	delivered := waitState(t, store, "duplicate-window", StateDelivered, nil)
	if delivered.Attempts != 2 || len(firstSink.Records())+len(secondSink.Records()) != 2 {
		t.Fatalf("duplicate window not demonstrated: attempts=%d deliveries=%d", delivered.Attempts, len(firstSink.Records())+len(secondSink.Records()))
	}
	shutdownService(t, second)
}

func TestServiceGracefulShutdownWaitsForInflight(t *testing.T) {
	store := newServiceStore(t)
	if _, err := store.Append(t.Context(), testRecord(testWithID("shutdown-record"))); err != nil {
		t.Fatal(err)
	}
	release := make(chan struct{})
	sink := &testBlockingSink{Started: make(chan Record, 1), Release: release}
	service := newTestService(t, store, sink, withOwner("shutdown-worker"))
	select {
	case <-sink.Started:
	case <-time.After(time.Second):
		t.Fatal("delivery did not start")
	}
	shutdownDone := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		shutdownDone <- service.Shutdown(ctx)
	}()
	select {
	case err := <-shutdownDone:
		t.Fatalf("shutdown returned before delivery release: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	if err := <-shutdownDone; err != nil {
		t.Fatal(err)
	}
	testRequireState(t, store, "shutdown-record", StateDelivered)
}

func TestServiceForcedShutdownIsBoundedAndReleasesLease(t *testing.T) {
	store := newServiceStore(t)
	if _, err := store.Append(t.Context(), testRecord(testWithID("forced-shutdown"), testWithMaxAttempts(3))); err != nil {
		t.Fatal(err)
	}
	sink := &testBlockingSink{Started: make(chan Record, 1)}
	service := newTestService(t, store, sink, withOwner("forced-shutdown-worker"))
	select {
	case <-sink.Started:
	case <-time.After(time.Second):
		t.Fatal("delivery did not start")
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	started := time.Now()
	if err := service.Shutdown(shutdownCtx); err != nil {
		t.Fatalf("forced Shutdown: %v", err)
	}
	if elapsed := time.Since(started); elapsed > 300*time.Millisecond {
		t.Fatalf("Shutdown exceeded its bound: %v", elapsed)
	}
	record := testRequireState(t, store, "forced-shutdown", StatePending)
	if record.LeaseToken != "" || record.LeaseUntil != nil {
		t.Fatalf("forced shutdown left an active lease: %#v", record)
	}
}

func TestServiceDeliveryRunsAfterClaimScope(t *testing.T) {
	store := newServiceStore(t)
	if _, err := store.Append(t.Context(), testRecord(testWithID("claim-scope"))); err != nil {
		t.Fatal(err)
	}
	observed := &claimScopeStore{IStore: store}
	sink := claimScopeSink{claimActive: &observed.active, delivered: make(chan error, 1)}
	service := newTestService(t, observed, sink, withOwner("claim-scope-worker"))
	select {
	case err := <-sink.delivered:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("delivery did not run")
	}
	waitState(t, store, "claim-scope", StateDelivered, nil)
	shutdownService(t, service)
}

func TestServiceMaximumAttemptsIsAHardCap(t *testing.T) {
	store := newServiceStore(t)
	if _, err := store.Append(t.Context(), testRecord(testWithID("service-attempt-cap"), testWithMaxAttempts(5))); err != nil {
		t.Fatal(err)
	}
	config := testServiceConfig()
	config.MaximumAttempts = 1
	sink := newTestScriptedSink(errors.New("still retryable"))
	service := newTestServiceWithConfig(t, store, sink, config, defaultRoute(),
		withOwner("attempt-cap-worker"),
		withRetryPolicy(testRetryPolicy{delay: time.Second, retry: true}),
	)
	dead := waitState(t, store, "service-attempt-cap", StateDead, nil)
	if dead.Attempts != 1 {
		t.Fatalf("attempts = %d, want 1", dead.Attempts)
	}
	shutdownService(t, service)
}

func TestServiceHonorsPersistedRecordAttemptCap(t *testing.T) {
	store := newServiceStore(t)
	if _, err := store.Append(t.Context(), testRecord(testWithID("record-attempt-cap"), testWithMaxAttempts(1))); err != nil {
		t.Fatal(err)
	}
	faults := newTestFaultStore(store)
	faults.Fail(testOperationRetry, 10, errors.New("Retry must not be called for an exhausted record"))
	config := testServiceConfig()
	config.MaximumAttempts = 5
	sink := newTestScriptedSink(errors.New("still retryable"))
	service := newTestServiceWithConfig(t, faults, sink, config, defaultRoute(),
		withOwner("record-cap-worker"),
		withRetryPolicy(testRetryPolicy{delay: time.Second, retry: true}),
	)
	dead := waitState(t, store, "record-attempt-cap", StateDead, nil)
	if dead.Attempts != 1 {
		t.Fatalf("attempts = %d, want 1", dead.Attempts)
	}
	shutdownService(t, service)
}

func TestServiceRegistrationConcurrency(t *testing.T) {
	store := newServiceStore(t)
	inputs := make([]NewRecord, 4)
	for index := range inputs {
		inputs[index] = testRecord(testWithID(ID(fmt.Sprintf("concurrency-%d", index))))
	}
	if _, err := store.Append(t.Context(), inputs...); err != nil {
		t.Fatal(err)
	}
	sink := &concurrencySink{release: make(chan struct{}), started: make(chan struct{}, 4)}
	config := testServiceConfig()
	config.WorkerCount = 4
	config.ClaimBatchSize = 4
	service := newTestServiceWithConfig(t, store, sink, config, DestinationsConfig{
		Destinations: []string{"events"}, Concurrency: 1,
	})
	select {
	case <-sink.started:
	case <-time.After(time.Second):
		t.Fatal("first delivery did not start")
	}
	time.Sleep(20 * time.Millisecond)
	if maximum := sink.maximum(); maximum != 1 {
		t.Fatalf("maximum registration concurrency = %d, want 1", maximum)
	}
	close(sink.release)
	for _, input := range inputs {
		waitState(t, store, input.ID, StateDelivered, nil)
	}
	shutdownService(t, service)
}

func TestServiceClaimDestinationsAreRegisteredAndCopied(t *testing.T) {
	store := newServiceStore(t)
	captured := &claimRequestCapture{IStore: store, requests: make(chan ClaimRequest, 2)}
	route := DestinationsConfig{Destinations: []string{"orders.confirmed"}}
	service := newTestServiceWithConfig(t, captured, &testRecordingSink{}, testServiceConfig(), route)
	route.Destinations[0] = "mutated.after.registration"
	first := receiveClaimRequest(t, captured.requests)
	if len(first.Destinations) != 1 || first.Destinations[0] != "orders.confirmed" {
		t.Fatalf("first claim destinations = %v", first.Destinations)
	}
	first.Destinations[0] = "mutated.by.store"
	second := receiveClaimRequest(t, captured.requests)
	if len(second.Destinations) != 1 || second.Destinations[0] != "orders.confirmed" {
		t.Fatalf("second claim destinations = %v", second.Destinations)
	}
	shutdownService(t, service)
}

func TestServiceRotatesLargeRoutingTableThroughBoundedClaims(t *testing.T) {
	store := newServiceStore(t)
	captured := &claimRequestCapture{IStore: store, requests: make(chan ClaimRequest, 2)}
	destinations := make([]string, MaxClaimDestinations+1)
	for index := range destinations {
		destinations[index] = fmt.Sprintf("destination-%02d", index)
	}
	service := newTestServiceWithConfig(t, captured, &testRecordingSink{}, testServiceConfig(), DestinationsConfig{Destinations: destinations})
	first := receiveClaimRequest(t, captured.requests)
	second := receiveClaimRequest(t, captured.requests)
	if len(first.Destinations) != MaxClaimDestinations || len(second.Destinations) != MaxClaimDestinations {
		t.Fatalf("bounded claim sizes = %d and %d", len(first.Destinations), len(second.Destinations))
	}
	seenLast := false
	for _, destination := range append(first.Destinations, second.Destinations...) {
		if destination == destinations[len(destinations)-1] {
			seenLast = true
		}
	}
	if !seenLast {
		t.Fatal("rotating claim windows did not reach the final registered destination")
	}
	shutdownService(t, service)
}

func TestServiceRejectsInvalidAndConflictingRegistrations(t *testing.T) {
	store := newServiceStore(t)
	service := newTestServiceWithConfig(t, store, nil, testServiceConfig(), DestinationsConfig{})
	sink := &testRecordingSink{}
	if err := service.Register(sink, DestinationsConfig{Destinations: []string{"orders", "orders"}}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("duplicate destination error = %v, want ErrInvalidArgument", err)
	}
	tooMany := make([]string, serviceLimit().MaxQueryValues+1)
	for index := range tooMany {
		tooMany[index] = fmt.Sprintf("destination-%03d", index)
	}
	if err := service.Register(sink, DestinationsConfig{Destinations: tooMany}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("oversized registration error = %v, want ErrInvalidArgument", err)
	}
	if err := service.Register(sink, DestinationsConfig{Destinations: []string{"orders"}}); err != nil {
		t.Fatal(err)
	}
	if err := service.Register(sink, DestinationsConfig{Destinations: []string{"orders"}}); !errors.Is(err, ErrConflict) {
		t.Fatalf("route conflict error = %v, want ErrConflict", err)
	}
	shutdownService(t, service)
}

func TestServiceTimeoutAndAmbiguousOutcomeRetry(t *testing.T) {
	t.Run("timeout", func(t *testing.T) {
		store := newServiceStore(t)
		if _, err := store.Append(t.Context(), testRecord(testWithID("timeout-record"))); err != nil {
			t.Fatal(err)
		}
		config := testServiceConfig()
		config.DeliveryTimeout = 20 * time.Millisecond
		service := newTestServiceWithConfig(t, store, blockingUntilCancelledSink{}, config, defaultRoute(),
			withOwner("timeout-worker"),
			withRetryPolicy(testRetryPolicy{delay: time.Second, retry: true}),
		)
		retried := waitState(t, store, "timeout-record", StatePending, func(record Record) bool { return record.Attempts == 1 })
		if retried.LastErrorCode != string(OutcomeRetryable) {
			t.Fatalf("timeout failure code = %q", retried.LastErrorCode)
		}
		shutdownService(t, service)
	})

	t.Run("ambiguous", func(t *testing.T) {
		store := newServiceStore(t)
		if _, err := store.Append(t.Context(), testRecord(testWithID("ambiguous-record"))); err != nil {
			t.Fatal(err)
		}
		sink := newTestScriptedSink(errors.New("connection lost before confirm"))
		service := newTestService(t, store, sink,
			withOwner("ambiguous-worker"),
			withErrorClassifier(ErrorClassifierFunc(func(error) DeliveryOutcome { return OutcomeAmbiguous })),
			withRetryPolicy(testRetryPolicy{delay: time.Second, retry: true}),
		)
		retried := waitState(t, store, "ambiguous-record", StatePending, func(record Record) bool { return record.Attempts == 1 })
		if retried.LastErrorCode != string(OutcomeAmbiguous) {
			t.Fatalf("ambiguous failure code = %q", retried.LastErrorCode)
		}
		shutdownService(t, service)
	})
}

func TestServiceLeaseRenewalAndLoss(t *testing.T) {
	t.Run("renewal", func(t *testing.T) {
		store := newServiceStore(t)
		if _, err := store.Append(t.Context(), testRecord(testWithID("renewal-record"))); err != nil {
			t.Fatal(err)
		}
		release := make(chan struct{})
		sink := &testBlockingSink{Started: make(chan Record, 1), Release: release}
		config := testServiceConfig()
		config.LeaseDuration = 80 * time.Millisecond
		config.LeaseRenewalThreshold = 40 * time.Millisecond
		config.DeliveryTimeout = 300 * time.Millisecond
		service := newTestServiceWithConfig(t, store, sink, config, defaultRoute(), withOwner("renew-worker"))
		select {
		case <-sink.Started:
		case <-time.After(time.Second):
			t.Fatal("delivery did not start")
		}
		time.Sleep(100 * time.Millisecond)
		close(release)
		waitState(t, store, "renewal-record", StateDelivered, nil)
		shutdownService(t, service)
	})

	t.Run("loss", func(t *testing.T) {
		store := newServiceStore(t)
		if _, err := store.Append(t.Context(), testRecord(testWithID("lease-loss-record"))); err != nil {
			t.Fatal(err)
		}
		faults := newTestFaultStore(store)
		faults.Fail(testOperationRenew, 1, ErrLeaseLost)
		config := testServiceConfig()
		config.LeaseDuration = 60 * time.Millisecond
		config.LeaseRenewalThreshold = 30 * time.Millisecond
		config.DeliveryTimeout = 300 * time.Millisecond
		service := newTestServiceWithConfig(t, faults, blockingUntilCancelledSink{}, config, defaultRoute(), withOwner("lease-loss-worker"))
		time.Sleep(50 * time.Millisecond)
		record := testRequireState(t, store, "lease-loss-record", StateLeased)
		if record.Attempts != 1 {
			t.Fatalf("attempts after lease loss = %d", record.Attempts)
		}
		shutdownService(t, service)
	})
}

type serviceOption func(*Config)

func withOwner(owner string) serviceOption {
	return func(config *Config) { config.Owner = owner }
}

func withRetryPolicy(policy IRetryPolicy) serviceOption {
	return func(config *Config) { config.RetryPolicy = policy }
}

func withErrorClassifier(classifier IErrorClassifier) serviceOption {
	return func(config *Config) { config.ErrorClassifier = classifier }
}

type testRetryPolicy struct {
	delay time.Duration
	retry bool
}

func (policy testRetryPolicy) Next(int, error) (time.Duration, bool) {
	return policy.delay, policy.retry
}

type blockingUntilCancelledSink struct{}

func (blockingUntilCancelledSink) Deliver(ctx context.Context, _ Record) error {
	<-ctx.Done()
	return ctx.Err()
}

type claimScopeStore struct {
	IStore
	active atomic.Bool
}

func (store *claimScopeStore) Claim(ctx context.Context, request ClaimRequest) ([]Record, error) {
	store.active.Store(true)
	defer store.active.Store(false)
	return store.IStore.Claim(ctx, request)
}

type claimScopeSink struct {
	claimActive *atomic.Bool
	delivered   chan error
}

func (sink claimScopeSink) Deliver(context.Context, Record) error {
	if sink.claimActive.Load() {
		err := errors.New("sink delivery ran while Claim was active")
		sink.delivered <- err
		return err
	}
	sink.delivered <- nil
	return nil
}

type concurrencySink struct {
	mu      sync.Mutex
	active  int
	max     int
	started chan struct{}
	release chan struct{}
}

func (sink *concurrencySink) Deliver(ctx context.Context, _ Record) error {
	sink.mu.Lock()
	sink.active++
	if sink.active > sink.max {
		sink.max = sink.active
	}
	sink.mu.Unlock()
	select {
	case sink.started <- struct{}{}:
	default:
	}
	select {
	case <-sink.release:
	case <-ctx.Done():
		return ctx.Err()
	}
	sink.mu.Lock()
	sink.active--
	sink.mu.Unlock()
	return nil
}

func (sink *concurrencySink) maximum() int {
	sink.mu.Lock()
	defer sink.mu.Unlock()
	return sink.max
}

type claimRequestCapture struct {
	IStore
	requests chan ClaimRequest
}

func (store *claimRequestCapture) Claim(ctx context.Context, request ClaimRequest) ([]Record, error) {
	copy := request
	copy.Destinations = append([]string(nil), request.Destinations...)
	select {
	case store.requests <- copy:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return nil, nil
}

func receiveClaimRequest(t testing.TB, requests <-chan ClaimRequest) ClaimRequest {
	t.Helper()
	select {
	case request := <-requests:
		return request
	case <-time.After(time.Second):
		t.Fatal("service did not issue a claim request")
		return ClaimRequest{}
	}
}

func newServiceStore(t testing.TB) *memoryTestStore {
	t.Helper()
	config := defaultMemoryTestStoreConfig()
	store, err := newMemoryTestStore(config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}

func testServiceConfig() Config {
	config := DefaultConfig()
	config.WorkerCount = 2
	config.ClaimBatchSize = 2
	config.LeaseDuration = time.Second
	config.LeaseRenewalThreshold = 250 * time.Millisecond
	config.PollMinimumInterval = time.Millisecond
	config.PollMaximumInterval = 5 * time.Millisecond
	config.DeliveryTimeout = 500 * time.Millisecond
	config.ShutdownTimeout = time.Second
	config.Retry.Minimum = time.Millisecond
	config.Retry.Maximum = time.Second
	return config
}

func serviceLimit() Limits { return DefaultLimits() }

func defaultRoute() DestinationsConfig {
	return DestinationsConfig{Destinations: []string{"events"}}
}

func newTestService(t testing.TB, store IStore, sink ISink, options ...serviceOption) *Service {
	return newTestServiceWithLease(t, store, sink, time.Second, options...)
}

func newTestServiceWithLease(t testing.TB, store IStore, sink ISink, lease time.Duration, options ...serviceOption) *Service {
	config := testServiceConfig()
	config.LeaseDuration = lease
	config.LeaseRenewalThreshold = lease / 4
	return newTestServiceWithConfig(t, store, sink, config, defaultRoute(), options...)
}

func newTestServiceWithConfig(t testing.TB, store IStore, sink ISink, config Config, route DestinationsConfig, options ...serviceOption) *Service {
	t.Helper()
	for _, option := range options {
		if option != nil {
			option(&config)
		}
	}
	feature := DefineStoreFeature("test", func(token gioc.Token) gioc.IProvider {
		return gioc.ValueProvider(token, IStore(store), true)
	})
	applicationModule := react.ApplicationModuleFor(react.ApplicationConfig{
		Parent: context.Background(), EnableShutDownHooks: true,
	})
	loggerModule := gioc.NewModule("OutboxServiceTestLogger").Global().Provide(
		react.Logger(author.Config{Level: author.FATAL}),
	)
	featureModule := ForFeature(feature, config)
	container := gioc.NewContainer()
	if err := container.AddModules(applicationModule, loggerModule, featureModule); err != nil {
		t.Fatal(err)
	}
	if err := container.Run(); err != nil {
		t.Fatal(err)
	}
	application, err := gioc.Get[*react.ApplicationService](container, react.ApplicationContextServiceToken)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(application.Shutdown)
	service, err := gioc.Get[*Service](container, OutboxServiceToken, featureModule)
	if err != nil {
		t.Fatal(err)
	}
	if len(route.Destinations) > 0 {
		if err = service.Register(sink, route); err != nil {
			t.Fatal(err)
		}
	}
	return service
}

func waitState(t testing.TB, store IReader, id ID, state State, predicate func(Record) bool) Record {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	for {
		record, err := store.Get(ctx, id)
		if err == nil && record.State == state && (predicate == nil || predicate(record)) {
			return record
		}
		select {
		case <-ctx.Done():
			t.Fatalf("wait for %q state %q: last=%#v err=%v", id, state, record, err)
		case <-time.After(time.Millisecond):
		}
	}
}

func shutdownService(t testing.TB, service *Service) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := service.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
}
