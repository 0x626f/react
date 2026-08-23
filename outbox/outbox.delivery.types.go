package outbox

import (
	"fmt"
	"strings"
	"time"
	"unicode/utf8"
)

// State is a durable delivery state.
type State string

const (
	StatePending   State = "pending"
	StateLeased    State = "leased"
	StateDelivered State = "delivered"
	StateDead      State = "dead"
	StateCancelled State = "cancelled"
)

// Valid reports whether s is a defined durable state.
func (s State) Valid() bool {
	switch s {
	case StatePending, StateLeased, StateDelivered, StateDead, StateCancelled:
		return true
	default:
		return false
	}
}

// Terminal reports whether records in s may be purged by retention policy.
func (s State) Terminal() bool {
	return s == StateDelivered || s == StateDead || s == StateCancelled
}

// DuplicateMode controls append behavior when an ID or idempotency key exists.
type DuplicateMode int

const (
	RejectDuplicate DuplicateMode = iota
	AcceptIdentical
)

// Valid reports whether m is a defined duplicate mode.
func (m DuplicateMode) Valid() bool { return m == RejectDuplicate || m == AcceptIdentical }

// ClaimRequest describes one bounded due-record lease operation.
type ClaimRequest struct {
	Owner         string
	Limit         int
	LeaseDuration time.Duration
	Destinations  []string
	// RecoveryLimit bounds expired-lease work performed by a claim. Zero uses
	// the adapter's claim limit.
	RecoveryLimit int
}

// LeaseRef is the complete fence needed to settle one delivery attempt.
type LeaseRef struct {
	ID      ID
	Owner   string
	Token   string
	Version uint64
}

// LeaseRef returns the complete fence from a claimed record snapshot.
func (r Record) LeaseRef() LeaseRef {
	return LeaseRef{ID: r.ID, Owner: r.LeaseOwner, Token: r.LeaseToken, Version: r.Version}
}

// DeliveryResult is intentionally transport-neutral. Delivery is considered
// successful only after the sink's own reliable-delivery condition is met.
type DeliveryResult struct{}

// Failure is bounded, non-sensitive error information persisted operationally.
type Failure struct {
	Code    string
	Message string
}

// RetryRequest persists one computed retry schedule and failure.
type RetryRequest struct {
	AvailableAt time.Time
	Failure     Failure
}

// RequeueOptions controls replay of a dead record.
type RequeueOptions struct {
	AvailableAt   time.Time
	ResetAttempts bool
	// MaxAttempts optionally replaces the existing limit. Zero preserves it.
	// When preserved attempts have reached that limit, requeue requires either
	// ResetAttempts or a larger valid MaxAttempts value.
	MaxAttempts int
}

// PurgeRequest deletes a bounded set of terminal records before a cutoff.
type PurgeRequest struct {
	States []State
	Before time.Time
	Limit  int
}

// NormalizePurgeRequest validates and canonicalizes bounded terminal cleanup
// work for adapter implementations.
func NormalizePurgeRequest(request PurgeRequest, limits Limits) (PurgeRequest, error) {
	limits = limits.withDefaults()
	request.Before = CanonicalTime(request.Before)
	if err := ValidateTimestamp("before", request.Before); err != nil {
		return PurgeRequest{}, err
	}
	if request.Limit < 1 || request.Limit > limits.MaxPurgeSize {
		return PurgeRequest{}, invalid("limit", fmt.Sprintf("must be between 1 and %d", limits.MaxPurgeSize))
	}
	if len(request.States) == 0 {
		request.States = []State{StateDelivered, StateDead, StateCancelled}
	}
	// There are exactly three terminal states. Bounding the input before
	// deduplication also bounds script and query construction work for hostile
	// duplicate lists.
	if len(request.States) > 3 {
		return PurgeRequest{}, invalid("states", "must contain at most three terminal states")
	}
	seen := make(map[State]struct{}, len(request.States))
	states := make([]State, 0, len(request.States))
	for _, state := range request.States {
		if !state.Terminal() {
			return PurgeRequest{}, ErrInvalidTransition
		}
		if _, exists := seen[state]; exists {
			continue
		}
		seen[state] = struct{}{}
		states = append(states, state)
	}
	request.States = states
	return request, nil
}

// BoundFailure truncates persisted failure fields without producing invalid UTF-8.
func BoundFailure(failure Failure, limits Limits) Failure {
	limits = limits.withDefaults()
	failure.Code = truncateUTF8(safeErrorText(failure.Code), limits.MaxErrorCodeBytes)
	failure.Message = truncateUTF8(safeErrorText(failure.Message), limits.MaxErrorMessageBytes)
	return failure
}

func safeErrorText(value string) string {
	value = strings.ToValidUTF8(value, "?")
	return strings.ReplaceAll(value, "\x00", "?")
}

func truncateUTF8(value string, maximum int) string {
	if len(value) <= maximum {
		return value
	}
	value = value[:maximum]
	for len(value) > 0 && !utf8.ValidString(value) {
		value = value[:len(value)-1]
	}
	return value
}
