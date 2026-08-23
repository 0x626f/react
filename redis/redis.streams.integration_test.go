package redis

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/0x626f/author"
	"github.com/0x626f/gioc"
	"github.com/0x626f/react"
)

type streamsIntegrationPayload struct {
	OrderID string `json:"order_id"`
}

func TestStreamsIntegrationPublishConsumeAndAck(t *testing.T) {
	_, streams := newStreamsIntegrationService(t, nil)
	stream := streamsIntegrationName("messages")
	group := streamsIntegrationName("group")
	cleanupStreamsIntegration(t, streams, stream)

	consumeCtx, cancel := context.WithCancel(t.Context())
	defer cancel()
	messages, err := streams.Consume(consumeCtx, group, stream)
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}
	if capacity := cap(messages); capacity != streams.config.ChannelSize {
		t.Fatalf("message channel capacity = %d, want %d", capacity, streams.config.ChannelSize)
	}
	if streams.pool.WorkerCount() != streams.config.WorkerCount {
		t.Fatalf("pool workers = %d, want %d", streams.pool.WorkerCount(), streams.config.WorkerCount)
	}

	want := streamsIntegrationPayload{OrderID: "order-42"}
	if err = streams.Publish(t.Context(), stream, "order-42", want); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	message := receiveStreamsIntegrationMessage(t, messages)
	if message.Stream != stream || message.Group != group || message.Key != "order-42" || message.Attempts != 1 {
		t.Fatalf("message metadata = %+v", message)
	}
	var got streamsIntegrationPayload
	if err = message.Decode(&got); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if got != want {
		t.Fatalf("decoded payload = %+v, want %+v", got, want)
	}
	modified := message
	modified.ID += "-modified"
	if err = streams.Ack(t.Context(), modified); err == nil {
		t.Fatal("Ack accepted modified delivery metadata")
	}
	if err = streams.Ack(t.Context(), message); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	if err = streams.Ack(t.Context(), message); err != nil {
		t.Fatalf("idempotent Ack: %v", err)
	}
	pending, err := streams.backend.Client().XPending(t.Context(), stream, group).Result()
	if err != nil {
		t.Fatalf("XPENDING: %v", err)
	}
	if pending.Count != 0 {
		t.Fatalf("pending count = %d, want 0", pending.Count)
	}
}

func TestStreamsIntegrationReclaimsUnacknowledgedMessage(t *testing.T) {
	_, streams := newStreamsIntegrationService(t, func(config *StreamsConfig) {
		config.ReclaimInterval = 25 * time.Millisecond
		config.ReclaimAfter = 75 * time.Millisecond
	})
	stream := streamsIntegrationName("reclaim")
	group := streamsIntegrationName("group")
	cleanupStreamsIntegration(t, streams, stream)

	firstCtx, cancelFirst := context.WithCancel(t.Context())
	first, err := streams.Consume(firstCtx, group, stream)
	if err != nil {
		t.Fatalf("first Consume: %v", err)
	}
	if err = streams.Publish(t.Context(), stream, "retry-key", map[string]string{"value": "retry"}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	initial := receiveStreamsIntegrationMessage(t, first)
	cancelFirst()
	waitStreamsIntegrationClosed(t, first)

	secondCtx, cancelSecond := context.WithCancel(t.Context())
	defer cancelSecond()
	second, err := streams.Consume(secondCtx, group, stream)
	if err != nil {
		t.Fatalf("second Consume: %v", err)
	}
	retried := receiveStreamsIntegrationMessage(t, second)
	if retried.ID != initial.ID || retried.Attempts != 2 {
		t.Fatalf("reclaimed message = %+v, initial = %+v", retried, initial)
	}
	if err = streams.Ack(t.Context(), initial); !errors.Is(err, ErrStreamsDeliveryLost) {
		t.Fatalf("Ack stale delivery = %v, want ErrStreamsDeliveryLost", err)
	}
	if err = streams.Ack(t.Context(), retried); err != nil {
		t.Fatalf("Ack reclaimed message: %v", err)
	}
}

func TestStreamsIntegrationWorkerCapacityAndDeadLetter(t *testing.T) {
	_, streams := newStreamsIntegrationService(t, func(config *StreamsConfig) {
		config.WorkerCount = 2
		config.DefaultConsumerCount = 1
		config.ReclaimInterval = 25 * time.Millisecond
		config.ReclaimAfter = 75 * time.Millisecond
		config.MaximumDeliveries = 1
	})
	stream := streamsIntegrationName("dead-letter")
	group := streamsIntegrationName("group")
	cleanupStreamsIntegration(t, streams, stream)

	allWorkersCtx, cancelAllWorkers := context.WithCancel(t.Context())
	allWorkers, err := streams.Consume(allWorkersCtx, group, stream, StreamsConsumerConfig{ConsumerCount: 2})
	if err != nil {
		t.Fatalf("Consume using all workers: %v", err)
	}
	_, err = streams.Consume(t.Context(), streamsIntegrationName("overflow-group"), streamsIntegrationName("overflow-stream"))
	if !errors.Is(err, ErrStreamsWorkerCapacity) {
		t.Fatalf("Consume beyond worker capacity = %v, want ErrStreamsWorkerCapacity", err)
	}
	cancelAllWorkers()
	waitStreamsIntegrationClosed(t, allWorkers)

	firstCtx, cancelFirst := context.WithCancel(t.Context())
	first, err := streams.Consume(firstCtx, group, stream)
	if err != nil {
		t.Fatalf("first dead-letter Consume: %v", err)
	}
	if err = streams.Publish(t.Context(), stream, "poison-key", map[string]string{"value": "poison"}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	_ = receiveStreamsIntegrationMessage(t, first)
	cancelFirst()
	waitStreamsIntegrationClosed(t, first)

	secondCtx, cancelSecond := context.WithCancel(t.Context())
	defer cancelSecond()
	second, err := streams.Consume(secondCtx, group, stream)
	if err != nil {
		t.Fatalf("second dead-letter Consume: %v", err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		length, lengthErr := streams.backend.Client().XLen(t.Context(), stream+streams.config.DeadLetterSuffix).Result()
		if lengthErr == nil && length == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("dead-letter stream length = %d, error = %v; want 1", length, lengthErr)
		}
		time.Sleep(20 * time.Millisecond)
	}
	select {
	case message := <-second:
		t.Fatalf("message exceeding maximum deliveries was routed again: %+v", message)
	default:
	}
}

func newStreamsIntegrationService(
	t *testing.T,
	configure func(*StreamsConfig),
) (*react.ApplicationService, *StreamsService) {
	t.Helper()
	rawURL := requireIntegrationURL(t)
	streamsConfig := DefaultStreamsConfig()
	streamsConfig.BlockTimeout = 50 * time.Millisecond
	if configure != nil {
		configure(&streamsConfig)
	}
	configModule := gioc.NewModule(streamsIntegrationName("config")).Global().Provide(
		gioc.ValueProvider(react.LoggerConfigToken, &react.LoggerModuleConfig{Level: author.NONE}, true),
		ProvideConfig(&Config{URL: rawURL}),
		ProvideStreamsConfig(&streamsConfig),
	)
	container := gioc.NewContainer()
	if err := container.AddModules(
		configModule,
		react.ApplicationModuleFor(react.ApplicationConfig{Parent: context.Background(), EnableShutDownHooks: true}),
		react.LoggerModule(),
		ForFeature(Streams),
	); err != nil {
		t.Fatalf("add modules: %v", err)
	}
	if err := container.Run(); err != nil {
		t.Fatalf("run container: %v", err)
	}
	application, err := gioc.Get[*react.ApplicationService](container, react.ApplicationContextServiceToken)
	if err != nil {
		t.Fatalf("resolve application service: %v", err)
	}
	streams, err := gioc.Get[*StreamsService](container, StreamsServiceToken)
	if err != nil {
		application.Shutdown()
		t.Fatalf("resolve streams service: %v", err)
	}
	t.Cleanup(application.Shutdown)
	return application, streams
}

func cleanupStreamsIntegration(t testing.TB, streams *StreamsService, stream string) {
	t.Helper()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := streams.backend.Client().Del(ctx, stream, stream+streams.config.DeadLetterSuffix).Err(); err != nil {
			t.Errorf("clean Redis streams integration keys: %v", err)
		}
	})
}

func receiveStreamsIntegrationMessage(t testing.TB, messages <-chan StreamsMessage) StreamsMessage {
	t.Helper()
	select {
	case message, open := <-messages:
		if !open {
			t.Fatal("Redis stream subscription closed before delivering a message")
		}
		return message
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for Redis stream message")
		return StreamsMessage{}
	}
}

func waitStreamsIntegrationClosed(t testing.TB, messages <-chan StreamsMessage) {
	t.Helper()
	select {
	case _, open := <-messages:
		if open {
			t.Fatal("Redis stream subscription delivered an unexpected message while closing")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for Redis stream subscription to close")
	}
}

func requireIntegrationURL(t testing.TB) string {
	t.Helper()
	const variable = "REDIS_TEST_URL"
	if value := os.Getenv(variable); value != "" {
		return value
	}
	if os.Getenv("REACT_REQUIRE_INTEGRATION") == "1" {
		t.Fatalf("%s not set while integration tests are required", variable)
	}
	t.Skipf("%s not set; skipping Redis integration test", variable)
	return ""
}

func streamsIntegrationName(prefix string) string {
	return fmt.Sprintf("react-integration-%s-%d", prefix, time.Now().UnixNano())
}
