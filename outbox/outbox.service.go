package outbox

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/0x626f/gioc"
	"github.com/0x626f/react"
)

type sinkRoute struct {
	sink    ISink
	permits chan struct{}
}

// Service is the outbox boundary exposed to applications. It provides the
// storage capabilities, owns all destination routing, and runs one bounded
// worker pool for every registered sink.
type Service struct {
	IStore

	ApplicationService *react.ApplicationService
	Logger             react.ILogger

	deliveryStore IDeliveryStore
	config        Config
	owner         string

	mu             sync.Mutex
	routes         map[string]*sinkRoute
	destinations   []string
	claimOffset    int
	started        bool
	stopping       bool
	claimCancel    context.CancelFunc
	deliveryCancel context.CancelFunc
	jobs           chan Record
	wake           chan struct{}
	done           chan struct{}
	wg             sync.WaitGroup
	inflight       map[ID]Record
}

var _ IStore = (*Service)(nil)

// Append delegates to the selected store and wakes the worker pool after a
// successful append. Transaction-bound adapter appends remain visible through
// the bounded polling interval after their outer transaction commits.
func (service *Service) Append(ctx context.Context, records ...NewRecord) ([]Record, error) {
	result, err := service.IStore.Append(ctx, records...)
	if err == nil && len(result) > 0 {
		service.signalWork()
	}
	return result, err
}

// AppendBatch delegates the atomic batch and wakes the worker pool after a
// successful append.
func (service *Service) AppendBatch(ctx context.Context, request AppendRequest) ([]Record, error) {
	result, err := service.IStore.AppendBatch(ctx, request)
	if err == nil && len(result) > 0 {
		service.signalWork()
	}
	return result, err
}

// NewService resolves the store, delivery capability, worker configuration,
// application lifecycle, and logger. It starts the service-owned worker pool;
// consumers then declare routes with Register.
func NewService(injections gioc.Injections) (*Service, error) {
	injections.Require(ServiceInjections...)
	store := gioc.MustResolve[IStore](OutboxStoreToken, injections)
	deliveryStore := gioc.MustResolve[IDeliveryStore](OutboxStoreToken, injections)
	configured := gioc.MustResolve[*Config](OutboxConfigToken, injections)
	application := gioc.MustResolve[*react.ApplicationService](react.ApplicationContextServiceToken, injections)
	logger := gioc.MustResolve[react.ILogger](react.LoggerToken, injections)
	if isNilValue(store) || isNilValue(deliveryStore) {
		return nil, invalid("store", "is required")
	}
	if configured == nil {
		return nil, invalid("config", "is required")
	}
	if application == nil {
		return nil, invalid("application", "is required")
	}
	if isNilValue(logger) {
		return nil, invalid("logger", "is required")
	}
	config, err := configured.normalized()
	if err != nil {
		return nil, err
	}
	owner := config.Owner
	if owner == "" {
		id, idErr := CryptoIDGenerator().NewID()
		if idErr != nil {
			return nil, idErr
		}
		owner = "outbox-" + string(id)
	}
	if err = ValidateLeaseOwner(owner, config.Limits); err != nil {
		return nil, err
	}

	service := &Service{
		IStore: store, ApplicationService: application, Logger: logger,
		deliveryStore: deliveryStore, config: config, owner: owner,
		routes: make(map[string]*sinkRoute), wake: make(chan struct{}, 1),
		inflight: make(map[ID]Record),
	}
	ctx, cancel := application.DeriveContext()
	if err = service.start(ctx); err != nil {
		cancel()
		return nil, err
	}
	application.AddPreShutdownHook(func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), config.ShutdownTimeout)
		defer shutdownCancel()
		if shutdownErr := service.Shutdown(shutdownCtx); shutdownErr != nil {
			service.Logger.Error("outbox shutdown failed: %v", shutdownErr)
		}
		cancel()
	})
	if closer, ok := store.(interface{ Close() error }); ok {
		var closeOnce sync.Once
		application.AddHook(func() {
			closeOnce.Do(func() {
				if closeErr := closer.Close(); closeErr != nil {
					service.Logger.Error("outbox store close failed: %v", closeErr)
				}
			})
		})
	}
	return service, nil
}

// Register atomically routes every configured destination to sink. A
// destination can belong to only one sink, which makes routing deterministic.
// Registration is safe while the worker pool is running.
func (service *Service) Register(sink ISink, config DestinationsConfig) error {
	if service == nil {
		return invalid("service", "is required")
	}
	if isNilValue(sink) {
		return invalid("sink", "is required")
	}
	config, err := config.normalized(service.config)
	if err != nil {
		return err
	}
	route := &sinkRoute{sink: sink, permits: make(chan struct{}, config.Concurrency)}

	service.mu.Lock()
	if service.stopping {
		service.mu.Unlock()
		return ErrClosed
	}
	if len(service.routes)+len(config.Destinations) > service.config.Limits.MaxQueryValues {
		service.mu.Unlock()
		return invalid("destinations", fmt.Sprintf("service supports at most %d registered destinations", service.config.Limits.MaxQueryValues))
	}
	for _, destination := range config.Destinations {
		if _, exists := service.routes[destination]; exists {
			service.mu.Unlock()
			return fmt.Errorf("%w: destination %q is already registered", ErrConflict, destination)
		}
	}
	for _, destination := range config.Destinations {
		service.routes[destination] = route
		service.destinations = append(service.destinations, destination)
	}
	sort.Strings(service.destinations)
	if service.claimOffset >= len(service.destinations) {
		service.claimOffset = 0
	}
	service.mu.Unlock()
	service.signalWork()
	return nil
}

// Destinations returns a stable copy of the registered routing keys.
func (service *Service) Destinations() []string {
	if service == nil {
		return nil
	}
	service.mu.Lock()
	defer service.mu.Unlock()
	return append([]string(nil), service.destinations...)
}

func (service *Service) start(parent context.Context) error {
	if parent == nil {
		return invalid("context", "is required")
	}
	if err := parent.Err(); err != nil {
		return err
	}
	service.mu.Lock()
	defer service.mu.Unlock()
	if service.started {
		if service.stopping {
			return ErrClosed
		}
		return nil
	}
	claimCtx, claimCancel := context.WithCancel(parent)
	deliveryCtx, deliveryCancel := context.WithCancel(context.WithoutCancel(parent))
	service.started = true
	service.claimCancel = claimCancel
	service.deliveryCancel = deliveryCancel
	service.jobs = make(chan Record, service.config.WorkerCount)
	service.done = make(chan struct{})
	for range service.config.WorkerCount {
		service.wg.Add(1)
		go service.worker(deliveryCtx)
	}
	service.wg.Add(1)
	go service.poll(claimCtx)
	go func() {
		select {
		case <-parent.Done():
			shutdownCtx, cancel := context.WithTimeout(context.Background(), service.config.ShutdownTimeout)
			defer cancel()
			if err := service.Shutdown(shutdownCtx); err != nil {
				service.Logger.Error("outbox shutdown failed: %v", err)
			}
		case <-service.done:
		}
	}()
	return nil
}

// Done closes after the worker pool and bounded lease cleanup stop.
func (service *Service) Done() <-chan struct{} {
	if service == nil {
		closed := make(chan struct{})
		close(closed)
		return closed
	}
	service.mu.Lock()
	defer service.mu.Unlock()
	if service.done == nil {
		closed := make(chan struct{})
		close(closed)
		return closed
	}
	return service.done
}

func (service *Service) poll(ctx context.Context) {
	defer service.wg.Done()
	defer close(service.jobs)
	interval := service.config.PollMinimumInterval
	for {
		if ctx.Err() != nil {
			return
		}
		destinations := service.nextClaimDestinations()
		if len(destinations) == 0 {
			if !service.wait(ctx, service.config.PollMaximumInterval) {
				return
			}
			continue
		}
		available := service.availableWorkerCapacity()
		if available <= 0 {
			if !service.wait(ctx, service.config.PollMinimumInterval) {
				return
			}
			continue
		}
		limit := min(service.config.ClaimBatchSize, available)
		records, err := service.deliveryStore.Claim(ctx, ClaimRequest{
			Owner: service.owner, Limit: limit,
			LeaseDuration: service.config.LeaseDuration,
			Destinations:  destinations,
			RecoveryLimit: limit,
		})
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			service.Logger.Error("outbox claim failed: %v", err)
			if !service.wait(ctx, interval) {
				return
			}
			interval = nextPollInterval(interval, service.config.PollMaximumInterval)
			continue
		}
		if len(records) == 0 {
			if !service.wait(ctx, interval) {
				return
			}
			interval = nextPollInterval(interval, service.config.PollMaximumInterval)
			continue
		}
		interval = service.config.PollMinimumInterval
		// Track the complete committed claim before handing any record to a
		// worker so shutdown can release even jobs still waiting in the channel.
		for _, record := range records {
			service.track(record)
		}
		for _, record := range records {
			select {
			case service.jobs <- record:
			case <-ctx.Done():
				return
			}
		}
	}
}

func (service *Service) worker(deliveryRoot context.Context) {
	defer service.wg.Done()
	for record := range service.jobs {
		service.deliver(deliveryRoot, record)
	}
}

func (service *Service) deliver(deliveryRoot context.Context, record Record) {
	route := service.routeFor(record.Destination)
	if route == nil {
		settleCtx, cancel := context.WithTimeout(context.Background(), service.settlementTimeout())
		defer cancel()
		service.Logger.Warning("outbox record %s has no route for destination %s", record.ID, record.Destination)
		service.releaseTracked(settleCtx, record)
		return
	}
	defer service.untrack(record.ID)
	lease := record.LeaseRef()
	attemptRoot, cancelAttempt := context.WithCancel(deliveryRoot)
	defer cancelAttempt()
	renewDone := make(chan error, 1)
	stopRenew := make(chan struct{})
	if service.config.RenewalEnabled {
		go service.renew(attemptRoot, cancelAttempt, record, stopRenew, renewDone)
	} else {
		close(renewDone)
	}

	select {
	case route.permits <- struct{}{}:
	case <-attemptRoot.Done():
		close(stopRenew)
		<-renewDone
		settleCtx, cancel := context.WithTimeout(context.Background(), service.settlementTimeout())
		defer cancel()
		if err := service.deliveryStore.Release(settleCtx, lease, CanonicalTime(service.config.Clock.Now())); err != nil && !errors.Is(err, ErrLeaseLost) {
			service.logSettlementError("release", err, record.Destination)
		}
		return
	}
	attemptCtx, cancelTimeout := context.WithTimeout(attemptRoot, service.config.DeliveryTimeout)
	err := route.sink.Deliver(attemptCtx, record.Clone())
	cancelTimeout()
	<-route.permits
	close(stopRenew)
	renewErr := <-renewDone
	if renewErr != nil {
		service.Logger.Warning("outbox lease renewal failed for destination %s: %v", record.Destination, renewErr)
		return
	}

	var outcome DeliveryOutcome
	if err == nil {
		outcome = OutcomeSuccess
	} else {
		outcome = service.config.ErrorClassifier.Classify(err)
		if !outcome.Valid() {
			outcome = OutcomeAmbiguous
		}
	}
	settleCtx, cancelSettle := context.WithTimeout(context.Background(), service.settlementTimeout())
	defer cancelSettle()
	switch outcome {
	case OutcomeSuccess:
		if settleErr := service.deliveryStore.Acknowledge(settleCtx, lease, DeliveryResult{}); settleErr != nil {
			service.logSettlementError("acknowledge", settleErr, record.Destination)
		}
	case OutcomeTerminal:
		if settleErr := service.deliveryStore.DeadLetter(settleCtx, lease, failureFor(outcome, err)); settleErr != nil {
			service.logSettlementError("dead_letter", settleErr, record.Destination)
		}
	case OutcomeRetryable, OutcomeAmbiguous:
		service.retryOrDeadLetter(settleCtx, record, outcome, err)
	default:
		service.retryOrDeadLetter(settleCtx, record, OutcomeAmbiguous, err)
	}
}

func (service *Service) retryOrDeadLetter(ctx context.Context, record Record, outcome DeliveryOutcome, failure error) {
	lease := record.LeaseRef()
	delay, retry := service.nextRetry(record, failure)
	if !retry {
		if err := service.deliveryStore.DeadLetter(ctx, lease, failureFor(outcome, failure)); err != nil {
			service.logSettlementError("dead_letter", err, record.Destination)
		}
		return
	}
	request := RetryRequest{
		AvailableAt: CanonicalTime(service.config.Clock.Now().Add(delay)),
		Failure:     failureFor(outcome, failure),
	}
	if err := service.deliveryStore.Retry(ctx, lease, request); err != nil {
		service.logSettlementError("retry", err, record.Destination)
	}
}

func (service *Service) renew(ctx context.Context, cancelDelivery context.CancelFunc, record Record, stop <-chan struct{}, done chan<- error) {
	defer close(done)
	leaseUntil := *record.LeaseUntil
	for {
		delay := leaseUntil.Sub(service.config.Clock.Now()) - service.config.LeaseRenewalThreshold
		if delay < time.Millisecond {
			delay = time.Millisecond
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			done <- nil
			return
		case <-stop:
			timer.Stop()
			done <- nil
			return
		case <-timer.C:
		}
		until := CanonicalTime(service.config.Clock.Now().Add(service.config.LeaseDuration))
		renewCtx, cancel := context.WithTimeout(ctx, minDuration(service.config.DeliveryTimeout, service.config.LeaseRenewalThreshold))
		err := service.deliveryStore.Renew(renewCtx, record.LeaseRef(), until)
		cancel()
		if err != nil {
			cancelDelivery()
			done <- err
			return
		}
		leaseUntil = until
	}
}

func (service *Service) nextClaimDestinations() []string {
	service.mu.Lock()
	defer service.mu.Unlock()
	count := len(service.destinations)
	if count == 0 {
		return nil
	}
	if count <= MaxClaimDestinations {
		return append([]string(nil), service.destinations...)
	}
	result := make([]string, MaxClaimDestinations)
	for index := range result {
		result[index] = service.destinations[(service.claimOffset+index)%count]
	}
	service.claimOffset = (service.claimOffset + MaxClaimDestinations) % count
	return result
}

func (service *Service) routeFor(destination string) *sinkRoute {
	service.mu.Lock()
	defer service.mu.Unlock()
	return service.routes[destination]
}

func (service *Service) signalWork() {
	select {
	case service.wake <- struct{}{}:
	default:
	}
}

func (service *Service) wait(ctx context.Context, duration time.Duration) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-service.wake:
		return true
	case <-timer.C:
		return true
	}
}

func (service *Service) track(record Record) {
	service.mu.Lock()
	service.inflight[record.ID] = record.Clone()
	service.mu.Unlock()
}

func (service *Service) availableWorkerCapacity() int {
	service.mu.Lock()
	defer service.mu.Unlock()
	return service.config.WorkerCount - len(service.inflight)
}

func (service *Service) untrack(id ID) {
	service.mu.Lock()
	delete(service.inflight, id)
	service.mu.Unlock()
	service.signalWork()
}

func (service *Service) releaseTracked(ctx context.Context, record Record) {
	if err := service.deliveryStore.Release(ctx, record.LeaseRef(), CanonicalTime(service.config.Clock.Now())); err != nil && !errors.Is(err, ErrLeaseLost) && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
		service.logSettlementError("release", err, record.Destination)
	}
	service.untrack(record.ID)
}

func (service *Service) releaseInflight(ctx context.Context) {
	service.mu.Lock()
	records := make([]Record, 0, len(service.inflight))
	for _, record := range service.inflight {
		records = append(records, record.Clone())
	}
	service.mu.Unlock()
	semaphore := make(chan struct{}, service.config.WorkerCount)
	var wait sync.WaitGroup
	for _, record := range records {
		if ctx.Err() != nil {
			break
		}
		wait.Add(1)
		go func(record Record) {
			defer wait.Done()
			select {
			case semaphore <- struct{}{}:
			case <-ctx.Done():
				return
			}
			defer func() { <-semaphore }()
			service.releaseTracked(ctx, record)
		}(record)
	}
	wait.Wait()
}

// Shutdown stops claims, drains in-flight work within ctx, and releases any
// still-current leases after forced cancellation. Calls are idempotent.
func (service *Service) Shutdown(ctx context.Context) error {
	if service == nil {
		return nil
	}
	if ctx == nil {
		return invalid("context", "is required")
	}
	service.mu.Lock()
	if !service.started {
		service.mu.Unlock()
		return nil
	}
	if service.stopping {
		done := service.done
		service.mu.Unlock()
		select {
		case <-done:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	service.stopping = true
	claimCancel := service.claimCancel
	deliveryCancel := service.deliveryCancel
	done := service.done
	service.mu.Unlock()

	claimCancel()
	waited := make(chan struct{})
	go func() { service.wg.Wait(); close(waited) }()
	forceCtx := ctx
	forceCancel := func() {}
	if deadline, ok := ctx.Deadline(); ok {
		remaining := time.Until(deadline)
		reserve := minDuration(100*time.Millisecond, remaining/10)
		if reserve > 0 && remaining > reserve {
			forceCtx, forceCancel = context.WithDeadline(ctx, deadline.Add(-reserve))
		}
	}
	defer forceCancel()
	var shutdownErr error
	forced := false
	select {
	case <-waited:
	case <-forceCtx.Done():
		forced = true
		deliveryCancel()
	}
	cleanupDone := make(chan struct{})
	go func() { service.releaseInflight(ctx); close(cleanupDone) }()
	if forced {
		select {
		case <-waited:
		case <-ctx.Done():
			shutdownErr = ctx.Err()
		}
	}
	select {
	case <-cleanupDone:
	case <-ctx.Done():
		if shutdownErr == nil {
			shutdownErr = ctx.Err()
		}
	}
	deliveryCancel()
	service.mu.Lock()
	select {
	case <-done:
	default:
		close(done)
	}
	service.mu.Unlock()
	return shutdownErr
}

func (service *Service) settlementTimeout() time.Duration {
	return minDuration(service.config.DeliveryTimeout, service.config.LeaseDuration/2)
}

func (service *Service) logSettlementError(operation string, err error, destination string) {
	service.Logger.Error("outbox %s failed for destination %s: %v", operation, destination, err)
}

func failureFor(outcome DeliveryOutcome, err error) Failure {
	message := ""
	if err != nil {
		message = err.Error()
	}
	return Failure{Code: string(outcome), Message: message}
}

func (service *Service) nextRetry(record Record, failure error) (time.Duration, bool) {
	if record.Attempts >= record.MaxAttempts || record.Attempts >= service.config.MaximumAttempts {
		return 0, false
	}
	return service.config.RetryPolicy.Next(record.Attempts, failure)
}

func minDuration(left, right time.Duration) time.Duration {
	if left < right {
		return left
	}
	return right
}

func nextPollInterval(current, maximum time.Duration) time.Duration {
	if current >= maximum || current > maximum/2 {
		return maximum
	}
	return minDuration(current*2, maximum)
}

// String returns a non-sensitive operational identity.
func (service *Service) String() string {
	if service == nil {
		return "outbox service"
	}
	return fmt.Sprintf("outbox service %s", service.owner)
}

func outboxServiceProvider() gioc.IProvider {
	return gioc.FactoryProvider(
		OutboxServiceToken,
		gioc.NewFactory(ServiceInjections, gioc.Singleton, NewService),
		true,
	)
}
