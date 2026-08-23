package outbox

import (
	"context"
	"time"
)

// Backlog is an operational estimate, not a readiness decision by itself.
type Backlog struct {
	Pending     int64
	Leased      int64
	Dead        int64
	OldestDueAt *time.Time
}

// Health separates storage and durability readiness from backlog signals.
type Health struct {
	Ready            bool
	StorageAvailable bool
	DurabilitySafe   bool
	Message          string
	Backlog          Backlog
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
