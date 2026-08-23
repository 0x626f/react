package outbox_test

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
	"github.com/0x626f/react/outbox"
	"github.com/0x626f/react/outbox/inmemory"
)

func TestServiceSuccessfulDelivery(t *testing.T) {
	store, clock := newServiceStore(t)
	record := outbox.TestRecord(outbox.TestWithID("delivery-success"))
	if _, err := store.Append(t.Context(), record); err != nil {
		t.Fatal(err)
	}
	sink := &outbox.TestRecordingSink{}
	service := newTestService(t, store, sink, clock, withOwner("success-worker"))

	delivered := waitState(t, store, record.ID, outbox.StateDelivered, nil)
	if delivered.Attempts != 1 || len(sink.Records()) != 1 {
		t.Fatalf("delivered=%#v calls=%d", delivered, len(sink.Records()))
	}
	shutdownService(t, service)
}

func TestServiceRetryAndTerminal(t *testing.T) {
	t.Run("retryable", func(t *testing.T) {
		store, clock := newServiceStore(t)
		if _, err := store.Append(t.Context(), outbox.TestRecord(outbox.TestWithID("delivery-retry"))); err != nil {
			t.Fatal(err)
		}
		sink := outbox.NewTestScriptedSink(errors.New("temporary"), nil)
		service := newTestService(t, store, sink, clock,
			withOwner("retry-worker"),
			withRetryPolicy(testRetryPolicy{delay: time.Second, retry: true}),
		)
		waitState(t, store, "delivery-retry", outbox.StatePending, func(record outbox.Record) bool { return record.Attempts == 1 })
		clock.Add(time.Second)
		waitState(t, store, "delivery-retry", outbox.StateDelivered, nil)
		if calls := len(sink.Calls()); calls != 2 {
			t.Fatalf("sink calls = %d, want 2", calls)
		}
		shutdownService(t, service)
	})

	t.Run("terminal", func(t *testing.T) {
		store, clock := newServiceStore(t)
		if _, err := store.Append(t.Context(), outbox.TestRecord(outbox.TestWithID("delivery-terminal"))); err != nil {
			t.Fatal(err)
		}
		sink := outbox.NewTestScriptedSink(&outbox.TerminalError{Err: errors.New("invalid route")})
		service := newTestService(t, store, sink, clock, withOwner("terminal-worker"))
		dead := waitState(t, store, "delivery-terminal", outbox.StateDead, nil)
		if dead.LastErrorCode != string(outbox.OutcomeTerminal) {
			t.Fatalf("failure code = %q", dead.LastErrorCode)
		}
		shutdownService(t, service)
	})
}

func TestServiceAckFailureLeavesAtLeastOnceDuplicateWindow(t *testing.T) {
	store, clock := newServiceStore(t)
	if _, err := store.Append(t.Context(), outbox.TestRecord(outbox.TestWithID("duplicate-window"), outbox.TestWithMaxAttempts(3))); err != nil {
		t.Fatal(err)
	}
	faults := outbox.NewTestFaultStore(store)
	faults.Fail(outbox.TestOperationAcknowledge, 1, errors.New("simulated crash after publish"))
	firstSink := &outbox.TestRecordingSink{}
	first := newTestService(t, faults, firstSink, clock, withOwner("first-worker"))
	waitState(t, store, "duplicate-window", outbox.StateLeased, func(outbox.Record) bool { return len(firstSink.Records()) == 1 })
	shutdownService(t, first)

	clock.Add(2 * time.Second)
	secondSink := &outbox.TestRecordingSink{}
	second := newTestServiceWithLease(t, store, secondSink, clock, time.Second, withOwner("second-worker"))
	delivered := waitState(t, store, "duplicate-window", outbox.StateDelivered, nil)
	if delivered.Attempts != 2 || len(firstSink.Records())+len(secondSink.Records()) != 2 {
		t.Fatalf("duplicate window not demonstrated: attempts=%d deliveries=%d", delivered.Attempts, len(firstSink.Records())+len(secondSink.Records()))
	}
	shutdownService(t, second)
}

func TestServiceGracefulShutdownWaitsForInflight(t *testing.T) {
	store, clock := newServiceStore(t)
	if _, err := store.Append(t.Context(), outbox.TestRecord(outbox.TestWithID("shutdown-record"))); err != nil {
		t.Fatal(err)
	}
	release := make(chan struct{})
	sink := &outbox.TestBlockingSink{Started: make(chan outbox.Record, 1), Release: release}
	service := newTestService(t, store, sink, clock, withOwner("shutdown-worker"))
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
	outbox.TestRequireState(t, store, "shutdown-record", outbox.StateDelivered)
}

func TestServiceForcedShutdownIsBoundedAndReleasesLease(t *testing.T) {
	store, clock := newServiceStore(t)
	if _, err := store.Append(t.Context(), outbox.TestRecord(outbox.TestWithID("forced-shutdown"), outbox.TestWithMaxAttempts(3))); err != nil {
		t.Fatal(err)
	}
	sink := &outbox.TestBlockingSink{Started: make(chan outbox.Record, 1)}
	service := newTestService(t, store, sink, clock, withOwner("forced-shutdown-worker"))
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
	record := outbox.TestRequireState(t, store, "forced-shutdown", outbox.StatePending)
	if record.LeaseToken != "" || record.LeaseUntil != nil {
		t.Fatalf("forced shutdown left an active lease: %#v", record)
	}
}

func TestServiceDeliveryRunsAfterClaimScope(t *testing.T) {
	store, clock := newServiceStore(t)
	if _, err := store.Append(t.Context(), outbox.TestRecord(outbox.TestWithID("claim-scope"))); err != nil {
		t.Fatal(err)
	}
	observed := &claimScopeStore{IStore: store}
	sink := claimScopeSink{claimActive: &observed.active, delivered: make(chan error, 1)}
	service := newTestService(t, observed, sink, clock, withOwner("claim-scope-worker"))
	select {
	case err := <-sink.delivered:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("delivery did not run")
	}
	waitState(t, store, "claim-scope", outbox.StateDelivered, nil)
	shutdownService(t, service)
}

func TestServiceMaximumAttemptsIsAHardCap(t *testing.T) {
	store, clock := newServiceStore(t)
	if _, err := store.Append(t.Context(), outbox.TestRecord(outbox.TestWithID("service-attempt-cap"), outbox.TestWithMaxAttempts(5))); err != nil {
		t.Fatal(err)
	}
	config := testServiceConfig()
	config.MaximumAttempts = 1
	sink := outbox.NewTestScriptedSink(errors.New("still retryable"))
	service := newTestServiceWithConfig(t, store, sink, clock, config, defaultRoute(),
		withOwner("attempt-cap-worker"),
		withRetryPolicy(testRetryPolicy{delay: time.Second, retry: true}),
	)
	dead := waitState(t, store, "service-attempt-cap", outbox.StateDead, nil)
	if dead.Attempts != 1 {
		t.Fatalf("attempts = %d, want 1", dead.Attempts)
	}
	shutdownService(t, service)
}

func TestServiceHonorsPersistedRecordAttemptCap(t *testing.T) {
	store, clock := newServiceStore(t)
	if _, err := store.Append(t.Context(), outbox.TestRecord(outbox.TestWithID("record-attempt-cap"), outbox.TestWithMaxAttempts(1))); err != nil {
		t.Fatal(err)
	}
	faults := outbox.NewTestFaultStore(store)
	faults.Fail(outbox.TestOperationRetry, 10, errors.New("Retry must not be called for an exhausted record"))
	config := testServiceConfig()
	config.MaximumAttempts = 5
	sink := outbox.NewTestScriptedSink(errors.New("still retryable"))
	service := newTestServiceWithConfig(t, faults, sink, clock, config, defaultRoute(),
		withOwner("record-cap-worker"),
		withRetryPolicy(testRetryPolicy{delay: time.Second, retry: true}),
	)
	dead := waitState(t, store, "record-attempt-cap", outbox.StateDead, nil)
	if dead.Attempts != 1 {
		t.Fatalf("attempts = %d, want 1", dead.Attempts)
	}
	shutdownService(t, service)
}

func TestServiceRegistrationConcurrency(t *testing.T) {
	store, clock := newServiceStore(t)
	inputs := make([]outbox.NewRecord, 4)
	for index := range inputs {
		inputs[index] = outbox.TestRecord(outbox.TestWithID(outbox.ID(fmt.Sprintf("concurrency-%d", index))))
	}
	if _, err := store.Append(t.Context(), inputs...); err != nil {
		t.Fatal(err)
	}
	sink := &concurrencySink{release: make(chan struct{}), started: make(chan struct{}, 4)}
	config := testServiceConfig()
	config.WorkerCount = 4
	config.ClaimBatchSize = 4
	service := newTestServiceWithConfig(t, store, sink, clock, config, outbox.DestinationsConfig{
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
		waitState(t, store, input.ID, outbox.StateDelivered, nil)
	}
	shutdownService(t, service)
}

func TestServiceClaimDestinationsAreRegisteredAndCopied(t *testing.T) {
	store, clock := newServiceStore(t)
	captured := &claimRequestCapture{IStore: store, requests: make(chan outbox.ClaimRequest, 2)}
	route := outbox.DestinationsConfig{Destinations: []string{"orders.confirmed"}}
	service := newTestServiceWithConfig(t, captured, &outbox.TestRecordingSink{}, clock, testServiceConfig(), route)
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
	store, clock := newServiceStore(t)
	captured := &claimRequestCapture{IStore: store, requests: make(chan outbox.ClaimRequest, 2)}
	destinations := make([]string, outbox.MaxClaimDestinations+1)
	for index := range destinations {
		destinations[index] = fmt.Sprintf("destination-%02d", index)
	}
	service := newTestServiceWithConfig(t, captured, &outbox.TestRecordingSink{}, clock, testServiceConfig(), outbox.DestinationsConfig{Destinations: destinations})
	first := receiveClaimRequest(t, captured.requests)
	second := receiveClaimRequest(t, captured.requests)
	if len(first.Destinations) != outbox.MaxClaimDestinations || len(second.Destinations) != outbox.MaxClaimDestinations {
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
	store, clock := newServiceStore(t)
	service := newTestServiceWithConfig(t, store, nil, clock, testServiceConfig(), outbox.DestinationsConfig{})
	sink := &outbox.TestRecordingSink{}
	if err := service.Register(sink, outbox.DestinationsConfig{Destinations: []string{"orders", "orders"}}); !errors.Is(err, outbox.ErrInvalidArgument) {
		t.Fatalf("duplicate destination error = %v, want ErrInvalidArgument", err)
	}
	tooMany := make([]string, serviceLimit().MaxQueryValues+1)
	for index := range tooMany {
		tooMany[index] = fmt.Sprintf("destination-%03d", index)
	}
	if err := service.Register(sink, outbox.DestinationsConfig{Destinations: tooMany}); !errors.Is(err, outbox.ErrInvalidArgument) {
		t.Fatalf("oversized registration error = %v, want ErrInvalidArgument", err)
	}
	if err := service.Register(sink, outbox.DestinationsConfig{Destinations: []string{"orders"}}); err != nil {
		t.Fatal(err)
	}
	if err := service.Register(sink, outbox.DestinationsConfig{Destinations: []string{"orders"}}); !errors.Is(err, outbox.ErrConflict) {
		t.Fatalf("route conflict error = %v, want ErrConflict", err)
	}
	shutdownService(t, service)
}

func TestServiceTimeoutAndAmbiguousOutcomeRetry(t *testing.T) {
	t.Run("timeout", func(t *testing.T) {
		store, clock := newServiceStore(t)
		if _, err := store.Append(t.Context(), outbox.TestRecord(outbox.TestWithID("timeout-record"))); err != nil {
			t.Fatal(err)
		}
		config := testServiceConfig()
		config.DeliveryTimeout = 20 * time.Millisecond
		service := newTestServiceWithConfig(t, store, blockingUntilCancelledSink{}, clock, config, defaultRoute(),
			withOwner("timeout-worker"),
			withRetryPolicy(testRetryPolicy{delay: time.Second, retry: true}),
		)
		retried := waitState(t, store, "timeout-record", outbox.StatePending, func(record outbox.Record) bool { return record.Attempts == 1 })
		if retried.LastErrorCode != string(outbox.OutcomeRetryable) {
			t.Fatalf("timeout failure code = %q", retried.LastErrorCode)
		}
		shutdownService(t, service)
	})

	t.Run("ambiguous", func(t *testing.T) {
		store, clock := newServiceStore(t)
		if _, err := store.Append(t.Context(), outbox.TestRecord(outbox.TestWithID("ambiguous-record"))); err != nil {
			t.Fatal(err)
		}
		sink := outbox.NewTestScriptedSink(errors.New("connection lost before confirm"))
		service := newTestService(t, store, sink, clock,
			withOwner("ambiguous-worker"),
			withErrorClassifier(outbox.ErrorClassifierFunc(func(error) outbox.DeliveryOutcome { return outbox.OutcomeAmbiguous })),
			withRetryPolicy(testRetryPolicy{delay: time.Second, retry: true}),
		)
		retried := waitState(t, store, "ambiguous-record", outbox.StatePending, func(record outbox.Record) bool { return record.Attempts == 1 })
		if retried.LastErrorCode != string(outbox.OutcomeAmbiguous) {
			t.Fatalf("ambiguous failure code = %q", retried.LastErrorCode)
		}
		shutdownService(t, service)
	})
}

func TestServiceLeaseRenewalAndLoss(t *testing.T) {
	t.Run("renewal", func(t *testing.T) {
		store, clock := newServiceStore(t)
		if _, err := store.Append(t.Context(), outbox.TestRecord(outbox.TestWithID("renewal-record"))); err != nil {
			t.Fatal(err)
		}
		release := make(chan struct{})
		sink := &outbox.TestBlockingSink{Started: make(chan outbox.Record, 1), Release: release}
		config := testServiceConfig()
		config.LeaseDuration = 80 * time.Millisecond
		config.LeaseRenewalThreshold = 40 * time.Millisecond
		config.DeliveryTimeout = 300 * time.Millisecond
		service := newTestServiceWithConfig(t, store, sink, clock, config, defaultRoute(), withOwner("renew-worker"))
		select {
		case <-sink.Started:
		case <-time.After(time.Second):
			t.Fatal("delivery did not start")
		}
		clock.Add(50 * time.Millisecond)
		time.Sleep(55 * time.Millisecond)
		clock.Add(50 * time.Millisecond)
		close(release)
		waitState(t, store, "renewal-record", outbox.StateDelivered, nil)
		shutdownService(t, service)
	})

	t.Run("loss", func(t *testing.T) {
		store, clock := newServiceStore(t)
		if _, err := store.Append(t.Context(), outbox.TestRecord(outbox.TestWithID("lease-loss-record"))); err != nil {
			t.Fatal(err)
		}
		faults := outbox.NewTestFaultStore(store)
		faults.Fail(outbox.TestOperationRenew, 1, outbox.ErrLeaseLost)
		config := testServiceConfig()
		config.LeaseDuration = 60 * time.Millisecond
		config.LeaseRenewalThreshold = 30 * time.Millisecond
		config.DeliveryTimeout = 300 * time.Millisecond
		service := newTestServiceWithConfig(t, faults, blockingUntilCancelledSink{}, clock, config, defaultRoute(), withOwner("lease-loss-worker"))
		time.Sleep(50 * time.Millisecond)
		record := outbox.TestRequireState(t, store, "lease-loss-record", outbox.StateLeased)
		if record.Attempts != 1 {
			t.Fatalf("attempts after lease loss = %d", record.Attempts)
		}
		shutdownService(t, service)
	})
}

type serviceOption func(*outbox.Config)

func withOwner(owner string) serviceOption {
	return func(config *outbox.Config) { config.Owner = owner }
}

func withRetryPolicy(policy outbox.IRetryPolicy) serviceOption {
	return func(config *outbox.Config) { config.RetryPolicy = policy }
}

func withErrorClassifier(classifier outbox.IErrorClassifier) serviceOption {
	return func(config *outbox.Config) { config.ErrorClassifier = classifier }
}

type testRetryPolicy struct {
	delay time.Duration
	retry bool
}

func (policy testRetryPolicy) Next(int, error) (time.Duration, bool) {
	return policy.delay, policy.retry
}

type blockingUntilCancelledSink struct{}

func (blockingUntilCancelledSink) Deliver(ctx context.Context, _ outbox.Record) error {
	<-ctx.Done()
	return ctx.Err()
}

type claimScopeStore struct {
	outbox.IStore
	active atomic.Bool
}

func (store *claimScopeStore) Claim(ctx context.Context, request outbox.ClaimRequest) ([]outbox.Record, error) {
	store.active.Store(true)
	defer store.active.Store(false)
	return store.IStore.Claim(ctx, request)
}

type claimScopeSink struct {
	claimActive *atomic.Bool
	delivered   chan error
}

func (sink claimScopeSink) Deliver(context.Context, outbox.Record) error {
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

func (sink *concurrencySink) Deliver(ctx context.Context, _ outbox.Record) error {
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
	outbox.IStore
	requests chan outbox.ClaimRequest
}

func (store *claimRequestCapture) Claim(ctx context.Context, request outbox.ClaimRequest) ([]outbox.Record, error) {
	copy := request
	copy.Destinations = append([]string(nil), request.Destinations...)
	select {
	case store.requests <- copy:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return nil, nil
}

func receiveClaimRequest(t testing.TB, requests <-chan outbox.ClaimRequest) outbox.ClaimRequest {
	t.Helper()
	select {
	case request := <-requests:
		return request
	case <-time.After(time.Second):
		t.Fatal("service did not issue a claim request")
		return outbox.ClaimRequest{}
	}
}

func newServiceStore(t testing.TB) (*inmemory.Store, *outbox.TestManualClock) {
	t.Helper()
	clock := outbox.NewTestManualClock(time.Date(2026, 2, 3, 4, 5, 6, 0, time.UTC))
	config := inmemory.DefaultConfig()
	config.Clock = clock
	config.TokenGenerator = outbox.NewTestSequenceGenerator("service-token")
	store, err := inmemory.NewStore(config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store, clock
}

func testServiceConfig() outbox.Config {
	config := outbox.DefaultConfig()
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

func serviceLimit() outbox.Limits { return outbox.DefaultLimits() }

func defaultRoute() outbox.DestinationsConfig {
	return outbox.DestinationsConfig{Destinations: []string{"events"}}
}

func newTestService(t testing.TB, store outbox.IStore, sink outbox.ISink, clock outbox.IClock, options ...serviceOption) *outbox.Service {
	return newTestServiceWithLease(t, store, sink, clock, time.Second, options...)
}

func newTestServiceWithLease(t testing.TB, store outbox.IStore, sink outbox.ISink, clock outbox.IClock, lease time.Duration, options ...serviceOption) *outbox.Service {
	config := testServiceConfig()
	config.LeaseDuration = lease
	config.LeaseRenewalThreshold = lease / 4
	return newTestServiceWithConfig(t, store, sink, clock, config, defaultRoute(), options...)
}

func newTestServiceWithConfig(t testing.TB, store outbox.IStore, sink outbox.ISink, clock outbox.IClock, config outbox.Config, route outbox.DestinationsConfig, options ...serviceOption) *outbox.Service {
	t.Helper()
	config.Clock = clock
	for _, option := range options {
		if option != nil {
			option(&config)
		}
	}
	feature := outbox.DefineStoreFeature("test", func(token gioc.Token) gioc.IProvider {
		return gioc.ValueProvider(token, outbox.IStore(store), true)
	})
	applicationModule := react.ApplicationModuleFor(react.ApplicationConfig{
		Parent: context.Background(), EnableShutDownHooks: true,
	})
	loggerModule := gioc.NewModule("OutboxServiceTestLogger").Global().Provide(
		react.Logger(author.Config{Level: author.FATAL}),
	)
	featureModule := outbox.ForFeature(feature, config)
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
	service, err := gioc.Get[*outbox.Service](container, outbox.OutboxServiceToken, featureModule)
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

func waitState(t testing.TB, store outbox.IReader, id outbox.ID, state outbox.State, predicate func(outbox.Record) bool) outbox.Record {
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

func shutdownService(t testing.TB, service *outbox.Service) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := service.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
}
