// InmemoryStore is the process-local reference implementation of the storage
// contract. It is concurrency-safe and deterministic when given
// deterministic clock and generators, but it is not durable: every record is
// lost when the process stops. Use it for tests, examples, local development,
// and applications that are explicitly ephemeral.
package outbox

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"
)

type entry struct {
	record    Record
	completed *LeaseRef
}

// InmemoryStore is a concurrency-safe process-local
type InmemoryStore struct {
	mu          sync.RWMutex
	config      InmemoryConfig
	records     map[ID]*entry
	idempotency map[string]ID
	closed      bool
}

// NewInmemoryStore validates and constructs a stopped process-local store.
func NewInmemoryStore(config InmemoryConfig) (*InmemoryStore, error) {
	config, err := config.normalized()
	if err != nil {
		return nil, err
	}
	return &InmemoryStore{config: config, records: make(map[ID]*entry), idempotency: make(map[string]ID)}, nil
}

// Close marks this facade closed without owning any external resource.
func (store *InmemoryStore) Close() error {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.closed = true
	return nil
}

func (store *InmemoryStore) Append(ctx context.Context, records ...NewRecord) ([]Record, error) {
	return store.AppendBatch(ctx, AppendRequest{Records: records, DuplicateMode: store.config.DuplicateMode})
}

func (store *InmemoryStore) AppendBatch(ctx context.Context, request AppendRequest) ([]Record, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !request.DuplicateMode.Valid() {
		return nil, fmt.Errorf("%w: duplicate mode", ErrInvalidArgument)
	}
	if len(request.Records) > store.config.Limits.MaxBatchSize {
		return nil, fmt.Errorf("%w: batch exceeds %d records", ErrInvalidArgument, store.config.Limits.MaxBatchSize)
	}
	now := CanonicalTime(store.config.Clock.Now())
	prepared := make([]Record, len(request.Records))
	for index, candidate := range request.Records {
		if candidate.ID == "" {
			id, err := store.config.IDGenerator.NewID()
			if err != nil {
				return nil, err
			}
			candidate.ID = id
		}
		record, err := PrepareRecord(candidate, now, store.config.DefaultMaxAttempts, store.config.Limits)
		if err != nil {
			return nil, err
		}
		prepared[index] = record
	}

	store.mu.Lock()
	defer store.mu.Unlock()
	if store.closed {
		return nil, ErrClosed
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	// Validate the whole batch against both persisted and batch-local indexes
	// before inserting anything.
	plannedByID := make(map[ID]Record)
	plannedByKey := make(map[string]ID)
	result := make([]Record, len(prepared))
	insert := make(map[ID]Record)
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
			if request.DuplicateMode == RejectDuplicate {
				return nil, ErrDuplicateID
			}
			if existing.ContentDigest != candidate.ContentDigest {
				return nil, ErrConflict
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

func (store *InmemoryStore) lookupDuplicate(candidate Record) (Record, bool) {
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
	return Record{}, false
}

func (store *InmemoryStore) Claim(ctx context.Context, request ClaimRequest) ([]Record, error) {
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
		if err = ValidateLeaseToken(token, store.config.Limits); err != nil {
			return nil, err
		}
		if _, exists := seenTokens[token]; exists {
			return nil, fmt.Errorf("%w: token generator returned a duplicate token", ErrInvalidArgument)
		}
		seenTokens[token] = struct{}{}
		tokens[index] = token
	}
	now := CanonicalTime(store.config.Clock.Now())
	leaseUntil := CanonicalTime(now.Add(request.LeaseDuration))
	if err := ValidateTimestamp("lease_until", leaseUntil); err != nil {
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
		return nil, ErrClosed
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	store.recoverExpired(now, recoveryLimit)
	candidates := make([]*entry, 0)
	for _, current := range store.records {
		record := current.record
		if record.State != StatePending || record.AvailableAt.After(now) || record.Attempts >= record.MaxAttempts {
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
	claimed := make([]Record, 0, len(candidates))
	for index, current := range candidates {
		current.record.State = StateLeased
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

func (store *InmemoryStore) validateClaim(request ClaimRequest) error {
	if err := ValidateLeaseOwner(request.Owner, store.config.Limits); err != nil {
		return err
	}
	if request.Limit < 1 || request.Limit > store.config.Limits.MaxClaimBatchSize {
		return fmt.Errorf("%w: claim limit", ErrInvalidArgument)
	}
	if err := ValidateLeaseDuration("lease_duration", request.LeaseDuration, store.config.MaxLeaseDuration); err != nil {
		return err
	}
	if request.RecoveryLimit == 0 {
		request.RecoveryLimit = request.Limit
	}
	if request.RecoveryLimit < 0 || request.RecoveryLimit > store.config.Limits.MaxClaimBatchSize {
		return fmt.Errorf("%w: recovery limit", ErrInvalidArgument)
	}
	if len(request.Destinations) > store.config.Limits.MaxQueryValues {
		return fmt.Errorf("%w: destination filters", ErrInvalidArgument)
	}
	for _, destination := range request.Destinations {
		if err := ValidateDestination(destination, store.config.Limits); err != nil {
			return err
		}
	}
	return nil
}

func (store *InmemoryStore) recoverExpired(now time.Time, limit int) {
	if limit == 0 {
		limit = store.config.Limits.MaxClaimBatchSize
	}
	expired := make([]*entry, 0)
	for _, current := range store.records {
		if current.record.State == StateLeased && current.record.LeaseUntil != nil && !current.record.LeaseUntil.After(now) {
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
			current.record.State = StateDead
			current.record.DeadAt = timePointer(now)
			current.record.LastErrorCode = "lease_expired_exhausted"
			current.record.LastErrorMessage = "lease expired after the final delivery attempt"
		} else {
			current.record.State = StatePending
			current.record.AvailableAt = now
			current.record.LastErrorCode = "lease_expired"
			current.record.LastErrorMessage = "delivery lease expired"
		}
		clearLease(&current.record)
		current.record.UpdatedAt = now
		current.record.Version++
	}
}

func (store *InmemoryStore) Renew(ctx context.Context, lease LeaseRef, until time.Time) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	now := CanonicalTime(store.config.Clock.Now())
	until = CanonicalTime(until)
	if err := ValidateTimestamp("lease_until", until); err != nil {
		return err
	}
	if !until.After(now) || until.After(now.Add(store.config.MaxLeaseDuration)) {
		return fmt.Errorf("%w: renewal deadline", ErrInvalidArgument)
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.closed {
		return ErrClosed
	}
	current, err := store.fenced(lease, now)
	if err != nil {
		return err
	}
	current.record.LeaseUntil = timePointer(until)
	current.record.UpdatedAt = now
	return nil
}

func (store *InmemoryStore) Acknowledge(ctx context.Context, lease LeaseRef, _ DeliveryResult) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	now := CanonicalTime(store.config.Clock.Now())
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.closed {
		return ErrClosed
	}
	current := store.records[lease.ID]
	if current != nil && current.record.State == StateDelivered {
		if current.completed != nil && *current.completed == lease {
			return nil
		}
		return ErrConflict
	}
	current, err := store.fenced(lease, now)
	if err != nil {
		return err
	}
	completed := lease
	current.completed = &completed
	current.record.State = StateDelivered
	current.record.DeliveredAt = timePointer(now)
	current.record.UpdatedAt = now
	current.record.Version++
	clearLease(&current.record)
	return nil
}

func (store *InmemoryStore) Retry(ctx context.Context, lease LeaseRef, retry RetryRequest) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	now := CanonicalTime(store.config.Clock.Now())
	availableAt := CanonicalTime(retry.AvailableAt)
	if err := ValidateTimestamp("available_at", availableAt); err != nil {
		return err
	}
	if availableAt.Before(now) {
		availableAt = now
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.closed {
		return ErrClosed
	}
	current, err := store.fenced(lease, now)
	if err != nil {
		return err
	}
	failure := BoundFailure(retry.Failure, store.config.Limits)
	if current.record.Attempts >= current.record.MaxAttempts {
		setDead(current, now, failure)
	} else {
		current.record.State = StatePending
		current.record.AvailableAt = availableAt
		current.record.LastErrorCode = failure.Code
		current.record.LastErrorMessage = failure.Message
		current.record.UpdatedAt = now
		current.record.Version++
		clearLease(&current.record)
	}
	return nil
}

func (store *InmemoryStore) DeadLetter(ctx context.Context, lease LeaseRef, failure Failure) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	now := CanonicalTime(store.config.Clock.Now())
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.closed {
		return ErrClosed
	}
	current, err := store.fenced(lease, now)
	if err != nil {
		return err
	}
	setDead(current, now, BoundFailure(failure, store.config.Limits))
	return nil
}

func (store *InmemoryStore) Release(ctx context.Context, lease LeaseRef, availableAt time.Time) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	now := CanonicalTime(store.config.Clock.Now())
	availableAt = CanonicalTime(availableAt)
	if err := ValidateTimestamp("available_at", availableAt); err != nil {
		return err
	}
	if availableAt.Before(now) {
		availableAt = now
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.closed {
		return ErrClosed
	}
	current, err := store.fenced(lease, now)
	if err != nil {
		return err
	}
	if current.record.Attempts >= current.record.MaxAttempts {
		setDead(current, now, Failure{Code: "released_exhausted", Message: "delivery lease released after the final attempt"})
	} else {
		current.record.State = StatePending
		current.record.AvailableAt = availableAt
		current.record.UpdatedAt = now
		current.record.Version++
		clearLease(&current.record)
	}
	return nil
}

func (store *InmemoryStore) fenced(lease LeaseRef, now time.Time) (*entry, error) {
	if lease.ID == "" || lease.Owner == "" || lease.Token == "" || lease.Version == 0 {
		return nil, ErrLeaseLost
	}
	current := store.records[lease.ID]
	if current == nil || current.record.State != StateLeased || current.record.LeaseUntil == nil || !current.record.LeaseUntil.After(now) || current.record.LeaseOwner != lease.Owner || current.record.LeaseToken != lease.Token || current.record.Version != lease.Version {
		return nil, ErrLeaseLost
	}
	return current, nil
}

func setDead(current *entry, now time.Time, failure Failure) {
	current.record.State = StateDead
	current.record.DeadAt = timePointer(now)
	current.record.LastErrorCode = failure.Code
	current.record.LastErrorMessage = failure.Message
	current.record.UpdatedAt = now
	current.record.Version++
	clearLease(&current.record)
}

func clearLease(record *Record) {
	record.LeaseOwner = ""
	record.LeaseToken = ""
	record.LeaseUntil = nil
}

func (store *InmemoryStore) Get(ctx context.Context, id ID) (Record, error) {
	if err := ctx.Err(); err != nil {
		return Record{}, err
	}
	if err := ValidateID(id, store.config.Limits); err != nil {
		return Record{}, err
	}
	store.mu.RLock()
	defer store.mu.RUnlock()
	if store.closed {
		return Record{}, ErrClosed
	}
	current := store.records[id]
	if current == nil {
		return Record{}, ErrNotFound
	}
	return current.record.Clone(), nil
}

func (store *InmemoryStore) Find(ctx context.Context, query Query) (Page, error) {
	if err := ctx.Err(); err != nil {
		return Page{}, err
	}
	query, cursor, err := NormalizeQuery(query, store.config.Limits)
	if err != nil {
		return Page{}, err
	}
	store.mu.RLock()
	defer store.mu.RUnlock()
	if store.closed {
		return Page{}, ErrClosed
	}
	records := make([]Record, 0, len(store.records))
	for _, current := range store.records {
		if matches(current.record, query) && RecordAfterCursor(current.record, cursor) {
			records = append(records, current.record.Clone())
		}
	}
	SortRecords(records, query.Sort, query.Direction)
	page := Page{}
	if len(records) > query.Limit {
		page.Records = records[:query.Limit]
		page.NextCursor, err = CursorForRecord(page.Records[len(page.Records)-1], query.Sort, query.Direction)
		if err != nil {
			return Page{}, err
		}
	} else {
		page.Records = records
	}
	return page, nil
}

func matches(record Record, query Query) bool {
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

func inRange(value time.Time, valueRange TimeRange) bool {
	if valueRange.From != nil && value.Before(*valueRange.From) {
		return false
	}
	if valueRange.To != nil && value.After(*valueRange.To) {
		return false
	}
	return true
}
func containsID(values []ID, value ID) bool {
	for _, candidate := range values {
		if candidate == value {
			return true
		}
	}
	return false
}
func containsState(values []State, value State) bool {
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

func (store *InmemoryStore) Cancel(ctx context.Context, id ID, reason string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := ValidateID(id, store.config.Limits); err != nil {
		return err
	}
	now := CanonicalTime(store.config.Clock.Now())
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.closed {
		return ErrClosed
	}
	current := store.records[id]
	if current == nil {
		return ErrNotFound
	}
	if current.record.State == StateCancelled {
		return nil
	}
	if current.record.State != StatePending {
		return ErrInvalidTransition
	}
	current.record.State = StateCancelled
	current.record.CancelledAt = timePointer(now)
	current.record.LastErrorCode = "cancelled"
	current.record.LastErrorMessage = BoundFailure(Failure{Message: reason}, store.config.Limits).Message
	current.record.UpdatedAt = now
	current.record.Version++
	return nil
}

func (store *InmemoryStore) Reschedule(ctx context.Context, id ID, availableAt time.Time) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := ValidateID(id, store.config.Limits); err != nil {
		return err
	}
	availableAt = CanonicalTime(availableAt)
	if err := ValidateTimestamp("available_at", availableAt); err != nil {
		return err
	}
	now := CanonicalTime(store.config.Clock.Now())
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.closed {
		return ErrClosed
	}
	current := store.records[id]
	if current == nil {
		return ErrNotFound
	}
	if current.record.State != StatePending {
		return ErrInvalidTransition
	}
	if current.record.AvailableAt.Equal(availableAt) {
		return nil
	}
	current.record.AvailableAt = availableAt
	current.record.UpdatedAt = now
	current.record.Version++
	return nil
}

func (store *InmemoryStore) Requeue(ctx context.Context, id ID, options RequeueOptions) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := ValidateID(id, store.config.Limits); err != nil {
		return err
	}
	now := CanonicalTime(store.config.Clock.Now())
	availableAt := CanonicalTime(options.AvailableAt)
	if availableAt.IsZero() {
		availableAt = now
	}
	if err := ValidateTimestamp("available_at", availableAt); err != nil {
		return err
	}
	if options.MaxAttempts < 0 || options.MaxAttempts > store.config.Limits.MaxAttempts {
		return fmt.Errorf("%w: max attempts", ErrInvalidArgument)
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.closed {
		return ErrClosed
	}
	current := store.records[id]
	if current == nil {
		return ErrNotFound
	}
	if current.record.State != StateDead {
		return ErrInvalidTransition
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
		return fmt.Errorf("%w: requeue requires ResetAttempts or MaxAttempts greater than preserved attempts", ErrInvalidArgument)
	}
	current.record.State = StatePending
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

func (store *InmemoryStore) Purge(ctx context.Context, request PurgeRequest) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	request, err := NormalizePurgeRequest(request, store.config.Limits)
	if err != nil {
		return 0, err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.closed {
		return 0, ErrClosed
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

func terminalTime(record Record) *time.Time {
	switch record.State {
	case StateDelivered:
		return record.DeliveredAt
	case StateDead:
		return record.DeadAt
	case StateCancelled:
		return record.CancelledAt
	default:
		return nil
	}
}

func (store *InmemoryStore) Backlog(ctx context.Context) (Backlog, error) {
	if err := ctx.Err(); err != nil {
		return Backlog{}, err
	}
	now := CanonicalTime(store.config.Clock.Now())
	store.mu.RLock()
	defer store.mu.RUnlock()
	if store.closed {
		return Backlog{}, ErrClosed
	}
	var backlog Backlog
	for _, current := range store.records {
		switch current.record.State {
		case StatePending:
			backlog.Pending++
			if !current.record.AvailableAt.After(now) && (backlog.OldestDueAt == nil || current.record.AvailableAt.Before(*backlog.OldestDueAt)) {
				backlog.OldestDueAt = timePointer(current.record.AvailableAt)
			}
		case StateLeased:
			backlog.Leased++
		case StateDead:
			backlog.Dead++
		}
	}
	return backlog, nil
}

func (store *InmemoryStore) Health(ctx context.Context) Health {
	backlog, err := store.Backlog(ctx)
	if err != nil {
		return Health{Ready: false, StorageAvailable: !errors.Is(err, ErrClosed), DurabilitySafe: false, Message: err.Error()}
	}
	return Health{Ready: true, StorageAvailable: true, DurabilitySafe: false, Message: "process-local, non-durable storage", Backlog: backlog}
}

func timePointer(value time.Time) *time.Time { value = CanonicalTime(value); return &value }
func stringSet(values []string) map[string]struct{} {
	result := make(map[string]struct{}, len(values))
	for _, value := range values {
		result[value] = struct{}{}
	}
	return result
}

var _ IStore = (*InmemoryStore)(nil)
var _ IBacklogReader = (*InmemoryStore)(nil)
var _ IHealthChecker = (*InmemoryStore)(nil)
