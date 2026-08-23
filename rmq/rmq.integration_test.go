package rmq

import (
	"context"
	"fmt"
	"net"
	neturl "net/url"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/0x626f/author"
	"github.com/0x626f/gioc"
	"github.com/0x626f/react"
	"github.com/rabbitmq/amqp091-go"
)

func TestRmqIntegrationPublishConsume(t *testing.T) {
	rawURL := os.Getenv("RMQ_TEST_URL")
	if rawURL == "" {
		t.Skip("set RMQ_TEST_URL, for example amqp://guest:guest@localhost:5672, to run RMQ integration test")
	}

	config := moduleConfigFromURL(t, rawURL)
	appModule := react.ApplicationModuleFor(react.ApplicationConfig{
		Parent:              context.Background(),
		EnableShutDownHooks: true,
	})
	configModule := gioc.NewModule("RmqTestConfig").Global().
		Provide(
			gioc.ValueProvider(react.LoggerConfigToken, &react.LoggerModuleConfig{Level: author.NONE}, true),
			ProvideModuleConfig(config),
		)

	container := gioc.NewContainer()
	if err := container.AddModules(configModule, appModule, react.LoggerModule(), testRmqModule()); err != nil {
		t.Fatalf("add modules: %v", err)
	}
	if err := container.Run(); err != nil {
		t.Fatalf("run container: %v", err)
	}

	app, err := gioc.Get[*react.ApplicationService](container, react.ApplicationContextServiceToken)
	if err != nil {
		t.Fatalf("resolve application service: %v", err)
	}
	t.Cleanup(app.Shutdown)

	rmqService, err := gioc.Get[*Service](container, ServiceToken)
	if err != nil {
		t.Fatalf("resolve rmq service: %v", err)
	}
	consumerService, err := gioc.Get[*ConsumerService](container, ConsumerServiceToken)
	if err != nil {
		t.Fatalf("resolve consumer service: %v", err)
	}
	producer := newTestProducer(t, container, app, rmqService)
	producer.Bind(&Exchange{})

	queue := &Queue{
		Name:       fmt.Sprintf("operator-rmq-test-%d", time.Now().UnixNano()),
		AutoDelete: true,
		Exclusive:  true,
	}
	if _, err := rmqService.CreateQueues(queue); err != nil {
		t.Fatalf("create queue: %v", err)
	}

	received := make(chan string, 1)
	consumer := &Consumer{
		Queue:   queue,
		Tag:     "operator-rmq-test",
		AutoAck: true,
		Handler: func(message IncomeMessage) error {
			received <- string(message.Body)
			return nil
		},
	}
	if err := consumerService.Consume(consumer); err != nil {
		t.Fatalf("consume: %v", err)
	}

	const payload = "rmq integration payload"
	if err := producer.Produce(&Publication{
		Destination: queue.Name,
		Message: amqp091.Publishing{
			ContentType: "text/plain",
			Body:        []byte(payload),
		},
	}); err != nil {
		t.Fatalf("produce: %v", err)
	}

	select {
	case got := <-received:
		if got != payload {
			t.Fatalf("received payload = %q, want %q", got, payload)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for consumed message")
	}
}

func TestRmqIntegrationManualAck(t *testing.T) {
	rawURL := os.Getenv("RMQ_TEST_URL")
	if rawURL == "" {
		t.Skip("set RMQ_TEST_URL, for example amqp://guest:guest@localhost:5672, to run RMQ integration test")
	}

	config := moduleConfigFromURL(t, rawURL)
	appModule := react.ApplicationModuleFor(react.ApplicationConfig{
		Parent:              context.Background(),
		EnableShutDownHooks: true,
	})
	configModule := gioc.NewModule("RmqManualAckTestConfig").Global().
		Provide(
			gioc.ValueProvider(react.LoggerConfigToken, &react.LoggerModuleConfig{Level: author.NONE}, true),
			ProvideModuleConfig(config),
		)

	container := gioc.NewContainer()
	if err := container.AddModules(configModule, appModule, react.LoggerModule(), testRmqModule()); err != nil {
		t.Fatalf("add modules: %v", err)
	}
	if err := container.Run(); err != nil {
		t.Fatalf("run container: %v", err)
	}

	app, err := gioc.Get[*react.ApplicationService](container, react.ApplicationContextServiceToken)
	if err != nil {
		t.Fatalf("resolve application service: %v", err)
	}
	t.Cleanup(app.Shutdown)

	rmqService, err := gioc.Get[*Service](container, ServiceToken)
	if err != nil {
		t.Fatalf("resolve rmq service: %v", err)
	}
	consumerService, err := gioc.Get[*ConsumerService](container, ConsumerServiceToken)
	if err != nil {
		t.Fatalf("resolve consumer service: %v", err)
	}
	producer := newTestProducer(t, container, app, rmqService)
	producer.Bind(&Exchange{})

	queue := &Queue{
		Name:       fmt.Sprintf("operator-rmq-manual-ack-test-%d", time.Now().UnixNano()),
		AutoDelete: true,
		Exclusive:  true,
	}
	if _, err := rmqService.CreateQueues(queue); err != nil {
		t.Fatalf("create queue: %v", err)
	}

	received := make(chan string, 1)
	ackErr := make(chan error, 1)
	consumer := &Consumer{
		Queue:   queue,
		Tag:     "operator-rmq-manual-ack-test",
		AutoAck: false,
		Handler: func(message IncomeMessage) error {
			if err := message.Ack(false); err != nil {
				ackErr <- err
				return err
			}
			received <- string(message.Body)
			return nil
		},
	}
	if err := consumerService.Consume(consumer); err != nil {
		t.Fatalf("consume: %v", err)
	}

	const payload = "manual ack payload"
	publishTestMessage(t, producer, queue, payload)
	assertReceivedMessage(t, received, payload)

	select {
	case err := <-ackErr:
		t.Fatalf("ack failed: %v", err)
	default:
	}

	channel, err := rmqService.Channel()
	if err != nil {
		t.Fatalf("create inspection channel: %v", err)
	}
	defer channel.Close()

	inspected, err := channel.QueueInspect(queue.Name)
	if err != nil {
		t.Fatalf("inspect queue: %v", err)
	}
	if inspected.Messages != 0 {
		t.Fatalf("queue has %d ready messages after ack, want 0", inspected.Messages)
	}
}

func TestRmqIntegrationReconnectProducerAndConsumer(t *testing.T) {
	rawURL := os.Getenv("RMQ_TEST_URL")
	if rawURL == "" {
		t.Skip("set RMQ_TEST_URL, for example amqp://guest:guest@localhost:5672, to run RMQ integration test")
	}

	config := moduleConfigFromURL(t, rawURL)
	config.RetryCount = 20
	config.RetryDelay = 100 * time.Millisecond

	appModule := react.ApplicationModuleFor(react.ApplicationConfig{
		Parent:              context.Background(),
		EnableShutDownHooks: true,
	})
	configModule := gioc.NewModule("RmqReconnectTestConfig").Global().
		Provide(
			gioc.ValueProvider(react.LoggerConfigToken, &react.LoggerModuleConfig{Level: author.NONE}, true),
			ProvideModuleConfig(config),
		)

	container := gioc.NewContainer()
	if err := container.AddModules(configModule, appModule, react.LoggerModule(), testRmqModule()); err != nil {
		t.Fatalf("add modules: %v", err)
	}
	if err := container.Run(); err != nil {
		t.Fatalf("run container: %v", err)
	}

	app, err := gioc.Get[*react.ApplicationService](container, react.ApplicationContextServiceToken)
	if err != nil {
		t.Fatalf("resolve application service: %v", err)
	}
	t.Cleanup(app.Shutdown)

	rmqService, err := gioc.Get[*Service](container, ServiceToken)
	if err != nil {
		t.Fatalf("resolve rmq service: %v", err)
	}
	queue := &Queue{
		Name: fmt.Sprintf("operator-rmq-reconnect-test-%d", time.Now().UnixNano()),
	}
	if _, err := rmqService.CreateQueues(queue); err != nil {
		t.Fatalf("create queue: %v", err)
	}
	t.Cleanup(func() {
		channel, err := rmqService.Channel()
		if err != nil {
			return
		}
		defer channel.Close()
		_, _ = channel.QueueDelete(queue.Name, false, false, false)
	})

	consumerService, err := gioc.Get[*ConsumerService](container, ConsumerServiceToken)
	if err != nil {
		t.Fatalf("resolve consumer service: %v", err)
	}
	received := make(chan string, 2)
	consumer := &Consumer{
		Queue:   queue,
		Tag:     "operator-rmq-reconnect-test",
		AutoAck: true,
		Handler: func(message IncomeMessage) error {
			received <- string(message.Body)
			return nil
		},
	}
	if err := consumerService.Consume(consumer); err != nil {
		t.Fatalf("consume: %v", err)
	}

	producer := newTestProducer(t, container, app, rmqService)
	producer.Bind(&Exchange{})

	publishTestMessage(t, producer, queue, "before reconnect")
	assertReceivedMessage(t, received, "before reconnect")

	if err := rmqService.closeConnection(); err != nil {
		t.Fatalf("close rmq connection: %v", err)
	}

	deadline := time.After(10 * time.Second)
	for {
		err = producer.Produce(&Publication{
			Destination: queue.Name,
			Message: amqp091.Publishing{
				ContentType: "text/plain",
				Body:        []byte("after reconnect"),
			},
		})
		if err == nil {
			break
		}

		select {
		case <-deadline:
			t.Fatalf("producer did not reconnect: %v", err)
		case <-time.After(100 * time.Millisecond):
		}
	}
	assertReceivedMessage(t, received, "after reconnect")
}

func TestRmqIntegrationRepeatedConnectionClose(t *testing.T) {
	rawURL := os.Getenv("RMQ_TEST_URL")
	if rawURL == "" {
		t.Skip("set RMQ_TEST_URL, for example amqp://guest:guest@localhost:5672, to run RMQ integration test")
	}

	config := moduleConfigFromURL(t, rawURL)
	config.RetryCount = 20
	config.RetryDelay = 100 * time.Millisecond

	appModule := react.ApplicationModuleFor(react.ApplicationConfig{
		Parent:              context.Background(),
		EnableShutDownHooks: true,
	})
	configModule := gioc.NewModule("RmqRepeatedCloseTestConfig").Global().
		Provide(
			gioc.ValueProvider(react.LoggerConfigToken, &react.LoggerModuleConfig{Level: author.NONE}, true),
			ProvideModuleConfig(config),
		)

	container := gioc.NewContainer()
	if err := container.AddModules(configModule, appModule, react.LoggerModule(), testRmqModule()); err != nil {
		t.Fatalf("add modules: %v", err)
	}
	if err := container.Run(); err != nil {
		t.Fatalf("run container: %v", err)
	}

	app, err := gioc.Get[*react.ApplicationService](container, react.ApplicationContextServiceToken)
	if err != nil {
		t.Fatalf("resolve application service: %v", err)
	}
	t.Cleanup(app.Shutdown)

	rmqService, err := gioc.Get[*Service](container, ServiceToken)
	if err != nil {
		t.Fatalf("resolve rmq service: %v", err)
	}
	producer := newTestProducer(t, container, app, rmqService)
	producer.Bind(&Exchange{})

	queue := &Queue{
		Name: fmt.Sprintf("operator-rmq-repeated-close-test-%d", time.Now().UnixNano()),
	}
	if _, err := rmqService.CreateQueues(queue); err != nil {
		t.Fatalf("create queue: %v", err)
	}
	t.Cleanup(func() {
		channel, err := rmqService.Channel()
		if err != nil {
			return
		}
		defer channel.Close()
		_, _ = channel.QueueDelete(queue.Name, false, false, false)
	})

	for i := 0; i < 3; i++ {
		_ = rmqService.closeConnection()
		_ = rmqService.closeConnection()

		payload := fmt.Sprintf("after close %d", i)
		waitPublishTestMessage(t, producer, queue, payload)
		assertQueueMessage(t, rmqService, queue, payload)
	}
}

func publishTestMessage(t *testing.T, producer *ProducerService, queue *Queue, payload string) {
	t.Helper()

	if err := producer.Produce(&Publication{
		Destination: queue.Name,
		Message: amqp091.Publishing{
			ContentType: "text/plain",
			Body:        []byte(payload),
		},
	}); err != nil {
		t.Fatalf("produce: %v", err)
	}
}

func waitPublishTestMessage(t *testing.T, producer *ProducerService, queue *Queue, payload string) {
	t.Helper()

	deadline := time.After(10 * time.Second)
	for {
		err := producer.Produce(&Publication{
			Destination: queue.Name,
			Message: amqp091.Publishing{
				ContentType: "text/plain",
				Body:        []byte(payload),
			},
		})
		if err == nil {
			return
		}

		select {
		case <-deadline:
			t.Fatalf("producer did not publish %q after reconnect: %v", payload, err)
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func assertReceivedMessage(t *testing.T, received <-chan string, want string) {
	t.Helper()

	select {
	case got := <-received:
		if got != want {
			t.Fatalf("received payload = %q, want %q", got, want)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for consumed message %q", want)
	}
}

func assertQueueMessage(t *testing.T, rmqService *Service, queue *Queue, want string) {
	t.Helper()

	deadline := time.After(5 * time.Second)
	for {
		channel, err := rmqService.Channel()
		if err == nil {
			message, ok, getErr := channel.Get(queue.Name, true)
			_ = channel.Close()
			if getErr != nil {
				t.Fatalf("get queue message: %v", getErr)
			}
			if ok {
				if got := string(message.Body); got != want {
					t.Fatalf("queue payload = %q, want %q", got, want)
				}
				return
			}
		}

		select {
		case <-deadline:
			t.Fatalf("timed out waiting for queue message %q", want)
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func testRmqModule() *gioc.Module {
	return gioc.NewModule("RmqTestModule").
		Provide(
			gioc.FactoryProvider(
				ServiceToken,
				gioc.NewFactory(
					ServiceInjections,
					gioc.Singleton,
					NewRmqService,
				),
				true,
			),
			gioc.FactoryProvider(
				ConsumerServiceToken,
				gioc.NewFactory(
					ConsumerServiceInjections,
					gioc.Singleton,
					NewConsumerService,
				),
				true,
			),
		)
}

func newTestProducer(t *testing.T, container *gioc.Container, app *react.ApplicationService, rmqService *Service) *ProducerService {
	t.Helper()

	logger, err := gioc.Get[react.ILogger](container, react.LoggerToken)
	if err != nil {
		t.Fatalf("resolve logger: %v", err)
	}

	producer, err := NewProducerService(gioc.Injections{
		{
			Token:    ServiceToken,
			Instance: rmqService,
		},
		{
			Token:    react.ApplicationContextServiceToken,
			Instance: app,
		},
		{
			Token:    react.LoggerToken,
			Instance: logger,
		},
	})
	if err != nil {
		t.Fatalf("create producer service: %v", err)
	}

	return producer
}

func moduleConfigFromURL(t *testing.T, rawURL string) *ModuleConfig {
	t.Helper()

	parsed, err := neturl.Parse(rawURL)
	if err != nil {
		t.Fatalf("parse RMQ_TEST_URL: %v", err)
	}
	if parsed.Scheme != "amqp" {
		t.Fatalf("RMQ_TEST_URL scheme = %q, want amqp", parsed.Scheme)
	}

	host := parsed.Hostname()
	port := 5672
	if parsed.Port() != "" {
		port, err = strconv.Atoi(parsed.Port())
		if err != nil {
			t.Fatalf("parse RMQ_TEST_URL port: %v", err)
		}
	}

	if ip := net.ParseIP(host); ip != nil {
		host = ip.String()
	}

	config := &ModuleConfig{
		Host:       host,
		Port:       port,
		RetryCount: 3,
		RetryDelay: 50 * time.Millisecond,
	}
	if parsed.User != nil {
		config.User = parsed.User.Username()
		config.Password, _ = parsed.User.Password()
	}
	if parsed.Path != "" {
		config.VirtualHost = strings.TrimPrefix(parsed.Path, "/")
	}

	return config
}

func TestModuleConfigFromURLVirtualHost(t *testing.T) {
	tests := []struct {
		name            string
		rawURL          string
		wantVirtualHost string
	}{
		{
			name:            "absent path",
			rawURL:          "amqp://guest:guest@localhost:5672",
			wantVirtualHost: "",
		},
		{
			name:            "named virtual host",
			rawURL:          "amqp://guest:guest@localhost:5672/operator",
			wantVirtualHost: "operator",
		},
		{
			name:            "encoded root virtual host",
			rawURL:          "amqp://guest:guest@localhost:5672/%2F",
			wantVirtualHost: "/",
		},
		{
			name:            "encoded slash in virtual host",
			rawURL:          "amqp://guest:guest@localhost:5672/team%2Fblue",
			wantVirtualHost: "team/blue",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			config := moduleConfigFromURL(t, test.rawURL)
			if config.VirtualHost != test.wantVirtualHost {
				t.Fatalf("moduleConfigFromURL(%q).VirtualHost = %q, want %q", test.rawURL, config.VirtualHost, test.wantVirtualHost)
			}
		})
	}
}
