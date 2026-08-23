package redis

import "context"

// IStreamsService is the lightweight application contract for Redis
// Streams. Consume uses manual acknowledgements and at-least-once delivery.
type IStreamsService interface {
	Publish(ctx context.Context, stream string, key string, value any) error
	Consume(ctx context.Context, group string, stream string, configs ...StreamsConsumerConfig) (<-chan StreamsMessage, error)
	Ack(ctx context.Context, message StreamsMessage) error
}
