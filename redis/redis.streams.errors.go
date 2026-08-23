package redis

import "errors"

var (
	// ErrStreamsClosed reports an operation attempted after the streams
	// service started shutting down.
	ErrStreamsClosed = errors.New("redis streams service is closed")
	// ErrStreamsWorkerCapacity reports that Consume requested more
	// long-lived Redis group readers than the service-owned pool has available.
	ErrStreamsWorkerCapacity = errors.New("redis streams worker capacity is exhausted")
	// ErrStreamsDeliveryLost reports that a message was reclaimed by another
	// consumer before this delivery could acknowledge it.
	ErrStreamsDeliveryLost = errors.New("redis stream delivery ownership was lost")
)
