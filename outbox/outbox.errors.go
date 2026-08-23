package outbox

import (
	"errors"
	"fmt"
)

var (
	// ErrNotFound means no record exists for the requested ID.
	ErrNotFound = errors.New("outbox: record not found")
	// ErrConflict means existing durable state contradicts an idempotent request.
	ErrConflict = errors.New("outbox: conflict")
	// ErrDuplicateID means an ID or idempotency key already exists in reject mode.
	ErrDuplicateID = errors.New("outbox: duplicate ID or idempotency key")
	// ErrLeaseLost means the supplied lease fence is absent, stale, or expired.
	ErrLeaseLost = errors.New("outbox: lease lost")
	// ErrInvalidTransition means the record state does not permit the operation.
	ErrInvalidTransition = errors.New("outbox: invalid state transition")
	// ErrUnsupportedCriteria means an adapter lacks indexes for a query shape.
	ErrUnsupportedCriteria = errors.New("outbox: unsupported query criteria")
	// ErrClosed means the adapter facade has been closed.
	ErrClosed = errors.New("outbox: store closed")
	// ErrInvalidArgument means validation rejected external input.
	ErrInvalidArgument = errors.New("outbox: invalid argument")
)

// FieldError identifies an invalid public input while remaining inspectable as
// ErrInvalidArgument through errors.Is.
type FieldError struct {
	Field   string
	Message string
}

func (e *FieldError) Error() string {
	if e == nil {
		return ErrInvalidArgument.Error()
	}
	if e.Field == "" {
		return fmt.Sprintf("%s: %s", ErrInvalidArgument, e.Message)
	}
	return fmt.Sprintf("%s: %s: %s", ErrInvalidArgument, e.Field, e.Message)
}

func (e *FieldError) Unwrap() error { return ErrInvalidArgument }

func invalid(field, message string) error {
	return &FieldError{Field: field, Message: message}
}

// OperationError adds stable operation context without hiding a sentinel
// returned by an adapter.
type OperationError struct {
	Operation string
	Err       error
}

func (e *OperationError) Error() string {
	if e == nil {
		return "outbox operation failed"
	}
	return fmt.Sprintf("outbox %s: %v", e.Operation, e.Err)
}

func (e *OperationError) Unwrap() error { return e.Err }
