package outbox

import "time"

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
