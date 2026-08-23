package outbox

import (
	"context"
	"errors"
)

// ISink delivers one copied record and returns nil only after its reliable
// transport acceptance condition has been met.
type ISink interface {
	Deliver(ctx context.Context, record Record) error
}

// SinkFunc adapts a delivery function to ISink.
type SinkFunc func(ctx context.Context, record Record) error

func (sink SinkFunc) Deliver(ctx context.Context, record Record) error {
	return sink(ctx, record)
}

// DeliveryOutcome classifies a transport attempt for settlement.
type DeliveryOutcome string

const (
	OutcomeSuccess   DeliveryOutcome = "success"
	OutcomeRetryable DeliveryOutcome = "retryable"
	OutcomeTerminal  DeliveryOutcome = "terminal"
	OutcomeAmbiguous DeliveryOutcome = "ambiguous"
)

// Valid reports whether outcome is one of the bounded built-in labels.
func (outcome DeliveryOutcome) Valid() bool {
	switch outcome {
	case OutcomeSuccess, OutcomeRetryable, OutcomeTerminal, OutcomeAmbiguous:
		return true
	default:
		return false
	}
}

// IErrorClassifier maps sink errors to transport-neutral outcomes.
type IErrorClassifier interface {
	Classify(err error) DeliveryOutcome
}

// ErrorClassifierFunc adapts a function to IErrorClassifier.
type ErrorClassifierFunc func(error) DeliveryOutcome

func (f ErrorClassifierFunc) Classify(err error) DeliveryOutcome { return f(err) }

// TerminalError lets a sink explicitly identify an error that retrying cannot
// fix. All unclassified and ambiguous errors are retried for at-least-once
// delivery.
type TerminalError struct{ Err error }

func (e *TerminalError) Error() string {
	if e == nil || e.Err == nil {
		return "terminal delivery error"
	}
	return e.Err.Error()
}
func (e *TerminalError) Unwrap() error { return e.Err }

// DefaultErrorClassifier treats TerminalError as terminal and every other
// non-nil error as retryable.
func DefaultErrorClassifier() IErrorClassifier {
	return ErrorClassifierFunc(func(err error) DeliveryOutcome {
		if err == nil {
			return OutcomeSuccess
		}
		var terminal *TerminalError
		if errors.As(err, &terminal) {
			return OutcomeTerminal
		}
		return OutcomeRetryable
	})
}
