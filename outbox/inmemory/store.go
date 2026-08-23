// Package inmemory provides the process-local reference implementation of the
// outbox storage contract. It is concurrency-safe and deterministic when given
// deterministic clock and generators, but it is not durable: every record is
// lost when the process stops. Use it for tests, examples, local development,
// and applications that are explicitly ephemeral.
package inmemory

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/0x626f/react/outbox"
)

// Config supplies deterministic dependencies, portable limits, and defaults.
type Config struct {
	Clock              outbox.IClock
	IDGenerator        outbox.IIDGenerator
	TokenGenerator     outbox.ITokenGenerator
	Limits             outbox.Limits
	DuplicateMode      outbox.DuplicateMode
	DefaultMaxAttempts int
	MaxLeaseDuration   time.Duration
}

// DefaultConfig returns a production-safe API configuration for ephemeral use.
func DefaultConfig() Config {
	return Config{
		Clock: outbox.SystemClock(), IDGenerator: outbox.CryptoIDGenerator(),
		TokenGenerator: outbox.CryptoTokenGenerator(), Limits: outbox.DefaultLimits(),
		DuplicateMode: outbox.RejectDuplicate, DefaultMaxAttempts: 10,
		MaxLeaseDuration: 5 * time.Minute,
	}
}

func (config Config) normalized() (Config, error) {
	defaults := DefaultConfig()
	if config.Clock == nil {
		config.Clock = defaults.Clock
	}
	if config.IDGenerator == nil {
		config.IDGenerator = defaults.IDGenerator
	}
	if config.TokenGenerator == nil {
		config.TokenGenerator = defaults.TokenGenerator
	}
	config.Limits = config.Limits.Normalized()
	if !config.DuplicateMode.Valid() {
		return Config{}, fmt.Errorf("%w: duplicate mode", outbox.ErrInvalidArgument)
	}
	if config.DefaultMaxAttempts == 0 {
		config.DefaultMaxAttempts = defaults.DefaultMaxAttempts
	}
	if config.DefaultMaxAttempts < 1 || config.DefaultMaxAttempts > config.Limits.MaxAttempts {
		return Config{}, fmt.Errorf("%w: default max attempts", outbox.ErrInvalidArgument)
	}
	if config.MaxLeaseDuration == 0 {
		config.MaxLeaseDuration = defaults.MaxLeaseDuration
	}
	if config.MaxLeaseDuration < time.Microsecond {
		return Config{}, fmt.Errorf("%w: max lease duration", outbox.ErrInvalidArgument)
	}
	return config, nil
}

type entry struct {
	record    outbox.Record
	completed *outbox.LeaseRef
}

// Store is a concurrency-safe process-local outbox.
type Store struct {
	mu          sync.RWMutex
	config      Config
	records     map[outbox.ID]*entry
	idempotency map[string]outbox.ID
	closed      bool
}

// NewStore validates and constructs a stopped process-local store.
func NewStore(config Config) (*Store, error) {
	config, err := config.normalized()
	if err != nil {
		return nil, err
	}
	return &Store{config: config, records: make(map[outbox.ID]*entry), idempotency: make(map[string]outbox.ID)}, nil
}

// New is an alias for NewStore.
func New(config Config) (*Store, error) { return NewStore(config) }

// Close marks this facade closed without owning any external resource.
func (store *Store) Close() error {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.closed = true
	return nil
}

func (store *Store) Append(ctx context.Context, records ...outbox.NewRecord) ([]outbox.Record, error) {
	return store.AppendBatch(ctx, outbox.AppendRequest{Records: records, DuplicateMode: store.config.DuplicateMode})
}

func (store *Store) AppendBatch(ctx context.Context, request outbox.AppendRequest) ([]outbox.Record, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !request.DuplicateMode.Valid() {
		return nil, fmt.Errorf("%w: duplicate mode", outbox.ErrInvalidArgument)
	}
	if len(request.Records) > store.config.Limits.MaxBatchSize {
		return nil, fmt.Errorf("%w: batch exceeds %d records", outbox.ErrInvalidArgument, store.config.Limits.MaxBatchSize)
	}
	now := outbox.CanonicalTime(store.config.Clock.Now())
	prepared := make([]outbox.Record, len(request.Records))
	for index, candidate := range request.Records {
		if candidate.ID == "" {
			id, err := store.config.IDGenerator.NewID()
			if err != nil {
				return nil, err
			}
			candidate.ID = id
		}
		record, err := outbox.PrepareRecord(candidate, now, store.config.DefaultMaxAttempts, store.config.Limits)
		if err != nil {
			return nil, err
		}
		prepared[index] = record
	}

	store.mu.Lock()
	defer store.mu.Unlock()
	if store.closed {
		return nil, outbox.ErrClosed
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	// Validate the whole batch against both persisted and batch-local indexes
	// before inserting anything.
	plannedByID := make(map[outbox.ID]outbox.Record)
	plannedByKey := make(map[string]outbox.ID)
	result := make([]outbox.Record, len(prepared))
	insert := make(map[outbox.ID]outbox.Record)
	for index, candidate := range prepared {
		existing, found := store.lookupDuplicate(candidate)
		if !found {
			if planned, ok := plannedByID[candidate.ID]; ok {
				existing, found = planned, true
			}
		}
		if !found && candidate.IdempotencyKey != "" {
			if id, ok := plannedByKey[candidate.IdempotencyKey]; ok {
				existing, found = plannedByID[id], true
			}
		}
		if found {
			if request.DuplicateMode == outbox.RejectDuplicate {
				return nil, outbox.ErrDuplicateID
			}
			if existing.ContentDigest != candidate.ContentDigest {
				return nil, outbox.ErrConflict
			}
			result[index] = existing.Clone()
			continue
		}
		plannedByID[candidate.ID] = candidate
		if candidate.IdempotencyKey != "" {
			plannedByKey[candidate.IdempotencyKey] = candidate.ID
		}
		insert[candidate.ID] = candidate
		result[index] = candidate.Clone()
	}
	for id, record := range insert {
		copy := record.Clone()
		store.records[id] = &entry{record: copy}
		if copy.IdempotencyKey != "" {
			store.idempotency[copy.IdempotencyKey] = id
		}
	}
	return result, nil
}

func (store *Store) lookupDuplicate(candidate outbox.Record) (outbox.Record, bool) {
	if existing := store.records[candidate.ID]; existing != nil {
		return existing.record, true
	}
	if candidate.IdempotencyKey != "" {
		if id, ok := store.idempotency[candidate.IdempotencyKey]; ok {
			if existing := store.records[id]; existing != nil {
				return existing.record, true
			}
		}
	}
	return outbox.Record{}, false
}

func (store *Store) Claim(ctx context.Context, request outbox.ClaimRequest) ([]outbox.Record, error) {
	if err := store.validateClaim(request); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	tokens := make([]string, request.Limit)
	seenTokens := make(map[string]struct{}, request.Limit)
	for index := range tokens {
		token, err := store.config.TokenGenerator.NewToken()
		if err != nil {
			return nil, err
		}
		if err = outbox.ValidateLeaseToken(token, store.config.Limits); err != nil {
			return nil, err
		}
		if _, exists := seenTokens[token]; exists {
			return nil, fmt.Errorf("%w: token generator returned a duplicate token", outbox.ErrInvalidArgument)
		}
		seenTokens[token] = struct{}{}
		tokens[index] = token
	}
	now := outbox.CanonicalTime(store.config.Clock.Now())
	leaseUntil := outbox.CanonicalTime(now.Add(request.LeaseDuration))
	if err := outbox.ValidateTimestamp("lease_until", leaseUntil); err != nil {
		return nil, err
	}
	destinations := stringSet(request.Destinations)
	recoveryLimit := request.RecoveryLimit
	if recoveryLimit == 0 {
		recoveryLimit = request.Limit
	}

	store.mu.Lock()
	defer store.mu.Unlock()
	if store.closed {
		return nil, outbox.ErrClosed
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	store.recoverExpired(now, recoveryLimit)
	candidates := make([]*entry, 0)
	for _, current := range store.records {
		record := current.record
		if record.State != outbox.StatePending || record.AvailableAt.After(now) || record.Attempts >= record.MaxAttempts {
			continue
		}
		if len(destinations) > 0 {
			if _, ok := destinations[record.Destination]; !ok {
				continue
			}
		}
		candidates = append(candidates, current)
	}
	sort.Slice(candidates, func(i, j int) bool {
		left, right := candidates[i].record, candidates[j].record
		if comparison := left.AvailableAt.Compare(right.AvailableAt); comparison != 0 {
			return comparison < 0
		}
		if comparison := left.CreatedAt.Compare(right.CreatedAt); comparison != 0 {
			return comparison < 0
		}
		return left.ID < right.ID
	})
	if len(candidates) > request.Limit {
		candidates = candidates[:request.Limit]
	}
	claimed := make([]outbox.Record, 0, len(candidates))
	for index, current := range candidates {
		current.record.State = outbox.StateLeased
		current.record.Attempts++
		current.record.LeaseOwner = request.Owner
		current.record.LeaseToken = tokens[index]
		current.record.LeaseUntil = timePointer(leaseUntil)
		current.record.UpdatedAt = now
		current.record.Version++
		current.completed = nil
		claimed = append(claimed, current.record.Clone())
	}
	return claimed, nil
}

func (store *Store) validateClaim(request outbox.ClaimRequest) error {
	if err := outbox.ValidateLeaseOwner(request.Owner, store.config.Limits); err != nil {
		return err
	}
	if request.Limit < 1 || request.Limit > store.config.Limits.MaxClaimBatchSize {
		return fmt.Errorf("%w: claim limit", outbox.ErrInvalidArgument)
	}
	if err := outbox.ValidateLeaseDuration("lease_duration", request.LeaseDuration, store.config.MaxLeaseDuration); err != nil {
		return err
	}
	if request.RecoveryLimit == 0 {
		request.RecoveryLimit = request.Limit
	}
	if request.RecoveryLimit < 0 || request.RecoveryLimit > store.config.Limits.MaxClaimBatchSize {
		return fmt.Errorf("%w: recovery limit", outbox.ErrInvalidArgument)
	}
	if len(request.Destinations) > store.config.Limits.MaxQueryValues {
		return fmt.Errorf("%w: destination filters", outbox.ErrInvalidArgument)
	}
	for _, destination := range request.Destinations {
		if err := outbox.ValidateDestination(destination, store.config.Limits); err != nil {
			return err
		}
	}
	return nil
}

func (store *Store) recoverExpired(now time.Time, limit int) {
	if limit == 0 {
		limit = store.config.Limits.MaxClaimBatchSize
	}
	expired := make([]*entry, 0)
	for _, current := range store.records {
		if current.record.State == outbox.StateLeased && current.record.LeaseUntil != nil && !current.record.LeaseUntil.After(now) {
			expired = append(expired, current)
		}
	}
	sort.Slice(expired, func(i, j int) bool {
		left, right := expired[i].record.LeaseUntil, expired[j].record.LeaseUntil
		if comparison := left.Compare(*right); comparison != 0 {
			return comparison < 0
		}
		return expired[i].record.ID < expired[j].record.ID
	})
	if len(expired) > limit {
		expired = expired[:limit]
	}
	for _, current := range expired {
		if current.record.Attempts >= current.record.MaxAttempts {
			current.record.State = outbox.StateDead
			current.record.DeadAt = timePointer(now)
			current.record.LastErrorCode = "lease_expired_exhausted"
			current.record.LastErrorMessage = "lease expired after the final delivery attempt"
		} else {
			current.record.State = outbox.StatePending
			current.record.AvailableAt = now
			current.record.LastErrorCode = "lease_expired"
			current.record.LastErrorMessage = "delivery lease expired"
		}
		clearLease(&current.record)
		current.record.UpdatedAt = now
		current.record.Version++
	}
}

func (store *Store) Renew(ctx context.Context, lease outbox.LeaseRef, until time.Time) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	now := outbox.CanonicalTime(store.config.Clock.Now())
	until = outbox.CanonicalTime(until)
	if err := outbox.ValidateTimestamp("lease_until", until); err != nil {
		return err
	}
	if !until.After(now) || until.After(now.Add(store.config.MaxLeaseDuration)) {
		return fmt.Errorf("%w: renewal deadline", outbox.ErrInvalidArgument)
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.closed {
		return outbox.ErrClosed
	}
	current, err := store.fenced(lease, now)
	if err != nil {
		return err
	}
	current.record.LeaseUntil = timePointer(until)
	current.record.UpdatedAt = now
	return nil
}

func (store *Store) Acknowledge(ctx context.Context, lease outbox.LeaseRef, _ outbox.DeliveryResult) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	now := outbox.CanonicalTime(store.config.Clock.Now())
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.closed {
		return outbox.ErrClosed
	}
	current := store.records[lease.ID]
	if current != nil && current.record.State == outbox.StateDelivered {
		if current.completed != nil && *current.completed == lease {
			return nil
		}
		return outbox.ErrConflict
	}
	current, err := store.fenced(lease, now)
	if err != nil {
		return err
	}
	completed := lease
	current.completed = &completed
	current.record.State = outbox.StateDelivered
	current.record.DeliveredAt = timePointer(now)
	current.record.UpdatedAt = now
	current.record.Version++
	clearLease(&current.record)
	return nil
}

func (store *Store) Retry(ctx context.Context, lease outbox.LeaseRef, retry outbox.RetryRequest) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	now := outbox.CanonicalTime(store.config.Clock.Now())
	availableAt := outbox.CanonicalTime(retry.AvailableAt)
	if err := outbox.ValidateTimestamp("available_at", availableAt); err != nil {
		return err
	}
	if availableAt.Before(now) {
		availableAt = now
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.closed {
		return outbox.ErrClosed
	}
	current, err := store.fenced(lease, now)
	if err != nil {
		return err
	}
	failure := outbox.BoundFailure(retry.Failure, store.config.Limits)
	if current.record.Attempts >= current.record.MaxAttempts {
		setDead(current, now, failure)
	} else {
		current.record.State = outbox.StatePending
		current.record.AvailableAt = availableAt
		current.record.LastErrorCode = failure.Code
		current.record.LastErrorMessage = failure.Message
		current.record.UpdatedAt = now
		current.record.Version++
		clearLease(&current.record)
	}
	return nil
}

func (store *Store) DeadLetter(ctx context.Context, lease outbox.LeaseRef, failure outbox.Failure) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	now := outbox.CanonicalTime(store.config.Clock.Now())
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.closed {
		return outbox.ErrClosed
	}
	current, err := store.fenced(lease, now)
	if err != nil {
		return err
	}
	setDead(current, now, outbox.BoundFailure(failure, store.config.Limits))
	return nil
}

func (store *Store) Release(ctx context.Context, lease outbox.LeaseRef, availableAt time.Time) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	now := outbox.CanonicalTime(store.config.Clock.Now())
	availableAt = outbox.CanonicalTime(availableAt)
	if err := outbox.ValidateTimestamp("available_at", availableAt); err != nil {
		return err
	}
	if availableAt.Before(now) {
		availableAt = now
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.closed {
		return outbox.ErrClosed
	}
	current, err := store.fenced(lease, now)
	if err != nil {
		return err
	}
	if current.record.Attempts >= current.record.MaxAttempts {
		setDead(current, now, outbox.Failure{Code: "released_exhausted", Message: "delivery lease released after the final attempt"})
	} else {
		current.record.State = outbox.StatePending
		current.record.AvailableAt = availableAt
		current.record.UpdatedAt = now
		current.record.Version++
		clearLease(&current.record)
	}
	return nil
}

func (store *Store) fenced(lease outbox.LeaseRef, now time.Time) (*entry, error) {
	if lease.ID == "" || lease.Owner == "" || lease.Token == "" || lease.Version == 0 {
		return nil, outbox.ErrLeaseLost
	}
	current := store.records[lease.ID]
	if current == nil || current.record.State != outbox.StateLeased || current.record.LeaseUntil == nil || !current.record.LeaseUntil.After(now) || current.record.LeaseOwner != lease.Owner || current.record.LeaseToken != lease.Token || current.record.Version != lease.Version {
		return nil, outbox.ErrLeaseLost
	}
	return current, nil
}

func setDead(current *entry, now time.Time, failure outbox.Failure) {
	current.record.State = outbox.StateDead
	current.record.DeadAt = timePointer(now)
	current.record.LastErrorCode = failure.Code
	current.record.LastErrorMessage = failure.Message
	current.record.UpdatedAt = now
	current.record.Version++
	clearLease(&current.record)
}

func clearLease(record *outbox.Record) {
	record.LeaseOwner = ""
	record.LeaseToken = ""
	record.LeaseUntil = nil
}

func (store *Store) Get(ctx context.Context, id outbox.ID) (outbox.Record, error) {
	if err := ctx.Err(); err != nil {
		return outbox.Record{}, err
	}
	if err := outbox.ValidateID(id, store.config.Limits); err != nil {
		return outbox.Record{}, err
	}
	store.mu.RLock()
	defer store.mu.RUnlock()
	if store.closed {
		return outbox.Record{}, outbox.ErrClosed
	}
	current := store.records[id]
	if current == nil {
		return outbox.Record{}, outbox.ErrNotFound
	}
	return current.record.Clone(), nil
}

func (store *Store) Find(ctx context.Context, query outbox.Query) (outbox.Page, error) {
	if err := ctx.Err(); err != nil {
		return outbox.Page{}, err
	}
	query, cursor, err := outbox.NormalizeQuery(query, store.config.Limits)
	if err != nil {
		return outbox.Page{}, err
	}
	store.mu.RLock()
	defer store.mu.RUnlock()
	if store.closed {
		return outbox.Page{}, outbox.ErrClosed
	}
	records := make([]outbox.Record, 0, len(store.records))
	for _, current := range store.records {
		if matches(current.record, query) && outbox.RecordAfterCursor(current.record, cursor) {
			records = append(records, current.record.Clone())
		}
	}
	outbox.SortRecords(records, query.Sort, query.Direction)
	page := outbox.Page{}
	if len(records) > query.Limit {
		page.Records = records[:query.Limit]
		page.NextCursor, err = outbox.CursorForRecord(page.Records[len(page.Records)-1], query.Sort, query.Direction)
		if err != nil {
			return outbox.Page{}, err
		}
	} else {
		page.Records = records
	}
	return page, nil
}

func matches(record outbox.Record, query outbox.Query) bool {
	if len(query.IDs) > 0 && !containsID(query.IDs, record.ID) {
		return false
	}
	if len(query.States) > 0 && !containsState(query.States, record.State) {
		return false
	}
	if len(query.Destinations) > 0 && !containsString(query.Destinations, record.Destination) {
		return false
	}
	if len(query.MessageTypes) > 0 && !containsString(query.MessageTypes, record.MessageType) {
		return false
	}
	if query.AggregateType != "" && query.AggregateType != record.AggregateType {
		return false
	}
	if query.AggregateID != "" && query.AggregateID != record.AggregateID {
		return false
	}
	if query.OrderingKey != "" && query.OrderingKey != record.OrderingKey {
		return false
	}
	if query.IdempotencyKey != "" && query.IdempotencyKey != record.IdempotencyKey {
		return false
	}
	if !inRange(record.CreatedAt, query.CreatedAt) || !inRange(record.AvailableAt, query.AvailableAt) {
		return false
	}
	return true
}

func inRange(value time.Time, valueRange outbox.TimeRange) bool {
	if valueRange.From != nil && value.Before(*valueRange.From) {
		return false
	}
	if valueRange.To != nil && value.After(*valueRange.To) {
		return false
	}
	return true
}
func containsID(values []outbox.ID, value outbox.ID) bool {
	for _, candidate := range values {
		if candidate == value {
			return true
		}
	}
	return false
}
func containsState(values []outbox.State, value outbox.State) bool {
	for _, candidate := range values {
		if candidate == value {
			return true
		}
	}
	return false
}
func containsString(values []string, value string) bool {
	for _, candidate := range values {
		if candidate == value {
			return true
		}
	}
	return false
}

func (store *Store) Cancel(ctx context.Context, id outbox.ID, reason string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := outbox.ValidateID(id, store.config.Limits); err != nil {
		return err
	}
	now := outbox.CanonicalTime(store.config.Clock.Now())
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.closed {
		return outbox.ErrClosed
	}
	current := store.records[id]
	if current == nil {
		return outbox.ErrNotFound
	}
	if current.record.State == outbox.StateCancelled {
		return nil
	}
	if current.record.State != outbox.StatePending {
		return outbox.ErrInvalidTransition
	}
	current.record.State = outbox.StateCancelled
	current.record.CancelledAt = timePointer(now)
	current.record.LastErrorCode = "cancelled"
	current.record.LastErrorMessage = outbox.BoundFailure(outbox.Failure{Message: reason}, store.config.Limits).Message
	current.record.UpdatedAt = now
	current.record.Version++
	return nil
}

func (store *Store) Reschedule(ctx context.Context, id outbox.ID, availableAt time.Time) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := outbox.ValidateID(id, store.config.Limits); err != nil {
		return err
	}
	availableAt = outbox.CanonicalTime(availableAt)
	if err := outbox.ValidateTimestamp("available_at", availableAt); err != nil {
		return err
	}
	now := outbox.CanonicalTime(store.config.Clock.Now())
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.closed {
		return outbox.ErrClosed
	}
	current := store.records[id]
	if current == nil {
		return outbox.ErrNotFound
	}
	if current.record.State != outbox.StatePending {
		return outbox.ErrInvalidTransition
	}
	if current.record.AvailableAt.Equal(availableAt) {
		return nil
	}
	current.record.AvailableAt = availableAt
	current.record.UpdatedAt = now
	current.record.Version++
	return nil
}

func (store *Store) Requeue(ctx context.Context, id outbox.ID, options outbox.RequeueOptions) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := outbox.ValidateID(id, store.config.Limits); err != nil {
		return err
	}
	now := outbox.CanonicalTime(store.config.Clock.Now())
	availableAt := outbox.CanonicalTime(options.AvailableAt)
	if availableAt.IsZero() {
		availableAt = now
	}
	if err := outbox.ValidateTimestamp("available_at", availableAt); err != nil {
		return err
	}
	if options.MaxAttempts < 0 || options.MaxAttempts > store.config.Limits.MaxAttempts {
		return fmt.Errorf("%w: max attempts", outbox.ErrInvalidArgument)
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.closed {
		return outbox.ErrClosed
	}
	current := store.records[id]
	if current == nil {
		return outbox.ErrNotFound
	}
	if current.record.State != outbox.StateDead {
		return outbox.ErrInvalidTransition
	}
	attempts := current.record.Attempts
	if options.ResetAttempts {
		attempts = 0
	}
	maximum := current.record.MaxAttempts
	if options.MaxAttempts > 0 {
		maximum = options.MaxAttempts
	}
	if maximum <= attempts {
		return fmt.Errorf("%w: requeue requires ResetAttempts or MaxAttempts greater than preserved attempts", outbox.ErrInvalidArgument)
	}
	current.record.State = outbox.StatePending
	current.record.AvailableAt = availableAt
	current.record.LastErrorCode = ""
	current.record.LastErrorMessage = ""
	current.record.DeadAt = nil
	if options.ResetAttempts {
		current.record.Attempts = 0
	}
	if options.MaxAttempts > 0 {
		current.record.MaxAttempts = options.MaxAttempts
	}
	current.record.UpdatedAt = now
	current.record.Version++
	current.completed = nil
	return nil
}

func (store *Store) Purge(ctx context.Context, request outbox.PurgeRequest) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	request, err := outbox.NormalizePurgeRequest(request, store.config.Limits)
	if err != nil {
		return 0, err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.closed {
		return 0, outbox.ErrClosed
	}
	candidates := make([]*entry, 0)
	for _, current := range store.records {
		if containsState(request.States, current.record.State) {
			transitioned := terminalTime(current.record)
			if transitioned != nil && transitioned.Before(request.Before) {
				candidates = append(candidates, current)
			}
		}
	}
	sort.Slice(candidates, func(i, j int) bool {
		left, right := terminalTime(candidates[i].record), terminalTime(candidates[j].record)
		if comparison := left.Compare(*right); comparison != 0 {
			return comparison < 0
		}
		return candidates[i].record.ID < candidates[j].record.ID
	})
	if len(candidates) > request.Limit {
		candidates = candidates[:request.Limit]
	}
	for _, current := range candidates {
		delete(store.records, current.record.ID)
		if current.record.IdempotencyKey != "" {
			delete(store.idempotency, current.record.IdempotencyKey)
		}
	}
	return len(candidates), nil
}

func terminalTime(record outbox.Record) *time.Time {
	switch record.State {
	case outbox.StateDelivered:
		return record.DeliveredAt
	case outbox.StateDead:
		return record.DeadAt
	case outbox.StateCancelled:
		return record.CancelledAt
	default:
		return nil
	}
}

func (store *Store) Backlog(ctx context.Context) (outbox.Backlog, error) {
	if err := ctx.Err(); err != nil {
		return outbox.Backlog{}, err
	}
	now := outbox.CanonicalTime(store.config.Clock.Now())
	store.mu.RLock()
	defer store.mu.RUnlock()
	if store.closed {
		return outbox.Backlog{}, outbox.ErrClosed
	}
	var backlog outbox.Backlog
	for _, current := range store.records {
		switch current.record.State {
		case outbox.StatePending:
			backlog.Pending++
			if !current.record.AvailableAt.After(now) && (backlog.OldestDueAt == nil || current.record.AvailableAt.Before(*backlog.OldestDueAt)) {
				backlog.OldestDueAt = timePointer(current.record.AvailableAt)
			}
		case outbox.StateLeased:
			backlog.Leased++
		case outbox.StateDead:
			backlog.Dead++
		}
	}
	return backlog, nil
}

func (store *Store) Health(ctx context.Context) outbox.Health {
	backlog, err := store.Backlog(ctx)
	if err != nil {
		return outbox.Health{Ready: false, StorageAvailable: !errors.Is(err, outbox.ErrClosed), DurabilitySafe: false, Message: err.Error()}
	}
	return outbox.Health{Ready: true, StorageAvailable: true, DurabilitySafe: false, Message: "process-local, non-durable storage", Backlog: backlog}
}

func timePointer(value time.Time) *time.Time { value = outbox.CanonicalTime(value); return &value }
func stringSet(values []string) map[string]struct{} {
	result := make(map[string]struct{}, len(values))
	for _, value := range values {
		result[value] = struct{}{}
	}
	return result
}

var _ outbox.IStore = (*Store)(nil)
var _ outbox.IBacklogReader = (*Store)(nil)
var _ outbox.IHealthChecker = (*Store)(nil)
