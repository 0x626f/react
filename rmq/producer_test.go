package rmq

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/rabbitmq/amqp091-go"
)

func TestProducerRequestContextTimeout(t *testing.T) {
	parent, stop := context.WithCancel(context.Background())
	defer stop()

	producer := &ProducerService{
		rmq: &Service{},
	}
	producer.rmq.Ctx = parent
	producer.WithTimeout(20 * time.Millisecond)

	ctx, cancel := producer.requestContext()
	defer cancel()

	select {
	case <-ctx.Done():
		if !errors.Is(ctx.Err(), context.DeadlineExceeded) {
			t.Fatalf("request context error = %v, want %v", ctx.Err(), context.DeadlineExceeded)
		}
	case <-time.After(time.Second):
		t.Fatal("request context did not expire")
	}
}

func TestProducerProduceUsesRequestTimeout(t *testing.T) {
	parent, stop := context.WithCancel(context.Background())
	defer stop()

	producer := &ProducerService{
		rmq:     &Service{},
		channel: &blockingPublishChannel{},
	}
	producer.rmq.Ctx = parent
	producer.WithTimeout(20 * time.Millisecond)
	producer.Bind(&Exchange{})

	err := producer.Produce(&Publication{
		Destination: "test-routing-key",
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Produce() error = %v, want %v", err, context.DeadlineExceeded)
	}
}

type blockingPublishChannel struct {
	closed bool
}

func (channel *blockingPublishChannel) NotifyClose(notifications chan Error) chan Error {
	return notifications
}

func (channel *blockingPublishChannel) IsClosed() bool {
	return channel.closed
}

func (channel *blockingPublishChannel) Close() error {
	channel.closed = true
	return nil
}

func (channel *blockingPublishChannel) PublishWithContext(ctx context.Context, _ string, _ string, _ bool, _ bool, _ OutcomeMessage) error {
	<-ctx.Done()
	return ctx.Err()
}

func (channel *blockingPublishChannel) QueueDeclare(string, bool, bool, bool, bool, Args) (amqp091.Queue, error) {
	return amqp091.Queue{}, nil
}

func (channel *blockingPublishChannel) ExchangeDeclare(string, string, bool, bool, bool, bool, Args) error {
	return nil
}

func (channel *blockingPublishChannel) QueueBind(string, string, string, bool, Args) error {
	return nil
}

func (channel *blockingPublishChannel) ConsumeWithContext(context.Context, string, string, bool, bool, bool, bool, Args) (<-chan amqp091.Delivery, error) {
	return nil, nil
}

func (channel *blockingPublishChannel) QueueDelete(string, bool, bool, bool) (int, error) {
	return 0, nil
}

func (channel *blockingPublishChannel) QueueInspect(string) (amqp091.Queue, error) {
	return amqp091.Queue{}, nil
}

func (channel *blockingPublishChannel) Get(string, bool) (amqp091.Delivery, bool, error) {
	return amqp091.Delivery{}, false, nil
}
