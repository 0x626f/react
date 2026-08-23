package outbox

import (
	"context"
	"time"
)

// IAppender atomically appends a bounded batch with its configured duplicate mode.
type IAppender interface {
	Append(ctx context.Context, records ...NewRecord) ([]Record, error)
}

// AppendRequest selects duplicate semantics for one atomic bounded batch.
type AppendRequest struct {
	Records       []NewRecord
	DuplicateMode DuplicateMode
}

// IBatchAppender is implemented by first-party adapters in addition to the
// convenient IAppender API.
type IBatchAppender interface {
	AppendBatch(ctx context.Context, request AppendRequest) ([]Record, error)
}

// IDeliveryStore exposes only fenced delivery state-machine operations.
type IDeliveryStore interface {
	Claim(ctx context.Context, request ClaimRequest) ([]Record, error)
	Renew(ctx context.Context, lease LeaseRef, until time.Time) error
	Acknowledge(ctx context.Context, lease LeaseRef, result DeliveryResult) error
	Retry(ctx context.Context, lease LeaseRef, retry RetryRequest) error
	DeadLetter(ctx context.Context, lease LeaseRef, failure Failure) error
	Release(ctx context.Context, lease LeaseRef, availableAt time.Time) error
}

// IReader retrieves records without granting mutation capability.
type IReader interface {
	Get(ctx context.Context, id ID) (Record, error)
	Find(ctx context.Context, query Query) (Page, error)
}

// IMaintenanceStore contains separately injectable administrative operations.
type IMaintenanceStore interface {
	Cancel(ctx context.Context, id ID, reason string) error
	Reschedule(ctx context.Context, id ID, availableAt time.Time) error
	Requeue(ctx context.Context, id ID, options RequeueOptions) error
	Purge(ctx context.Context, request PurgeRequest) (int, error)
}

// IStore is the aggregate convenience contract implemented by first-party adapters.
type IStore interface {
	IAppender
	IBatchAppender
	IDeliveryStore
	IReader
	IMaintenanceStore
}

// ISink delivers one copied record and returns nil only after its reliable
// transport acceptance condition has been met.
type ISink interface {
	Deliver(ctx context.Context, record Record) error
}

// IErrorClassifier maps sink errors to transport-neutral outcomes.
type IErrorClassifier interface {
	Classify(err error) DeliveryOutcome
}

// IRetryPolicy decides whether and when a failed attempt becomes due again.
type IRetryPolicy interface {
	Next(attempt int, failure error) (delay time.Duration, retry bool)
}

// IHealthChecker reports readiness separately from backlog pressure. A large
// backlog is observable but does not by itself make Ready false.
type IHealthChecker interface {
	Health(ctx context.Context) Health
}

// IBacklogReader supplies bounded-cardinality backlog observations.
type IBacklogReader interface {
	Backlog(ctx context.Context) (Backlog, error)
}
