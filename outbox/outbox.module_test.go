package outbox

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/0x626f/author"
	"github.com/0x626f/gioc"
	"github.com/0x626f/react"
)

type moduleSink struct{ delivered chan Record }

func (sink *moduleSink) Deliver(ctx context.Context, record Record) error {
	select {
	case sink.delivered <- record.Clone():
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func TestForFeatureInitializesServiceAndProvidesCapabilities(t *testing.T) {
	store := newServiceStore(t)
	workerConfig := moduleServiceConfig()
	featureModule := ForFeature(moduleTestStoreFeature(store), workerConfig)
	container, application := runOutboxContainer(t, featureModule)
	var shutdownOnce sync.Once
	shutdown := func() { shutdownOnce.Do(application.Shutdown) }
	t.Cleanup(shutdown)

	service, err := gioc.Get[*Service](container, OutboxServiceToken, featureModule)
	if err != nil {
		t.Fatalf("resolve outbox Service = %v", err)
	}
	if service.ApplicationService != application {
		t.Fatal("outbox Service did not receive React's ApplicationService")
	}
	if service.Logger == nil {
		t.Fatal("outbox Service did not resolve ILogger")
	}
	resolvedStore, err := gioc.Get[IStore](container, OutboxStoreToken, featureModule)
	if err != nil {
		t.Fatalf("resolve module store = %v", err)
	}
	if service.IStore != resolvedStore || resolvedStore != store {
		t.Fatal("Service did not receive the module store")
	}

	appender, err := gioc.Get[IAppender](container, OutboxAppenderToken, featureModule)
	if err != nil {
		t.Fatalf("resolve IAppender = %v", err)
	}
	reader, err := gioc.Get[IReader](container, OutboxReaderToken, featureModule)
	if err != nil {
		t.Fatalf("resolve IReader = %v", err)
	}
	deliveryStore, err := gioc.Get[IDeliveryStore](container, OutboxDeliveryStoreToken, featureModule)
	if err != nil {
		t.Fatalf("resolve IDeliveryStore = %v", err)
	}
	maintenance, err := gioc.Get[IMaintenanceStore](container, OutboxMaintenanceStoreToken, featureModule)
	if err != nil {
		t.Fatalf("resolve IMaintenanceStore = %v", err)
	}
	if appender != service || reader != service || deliveryStore != service || maintenance != service {
		t.Fatal("capability providers must expose the outbox Service")
	}

	sink := &moduleSink{delivered: make(chan Record, 1)}
	if err = service.Register(sink, DestinationsConfig{Destinations: []string{"orders.confirmed"}}); err != nil {
		t.Fatal(err)
	}
	records, err := appender.Append(t.Context(), NewRecord{
		ID: "module-record", Destination: "orders.confirmed",
		MessageType: "order.confirmed", Payload: []byte(`{"order_id":"42"}`),
	})
	if err != nil {
		t.Fatalf("Append() = %v", err)
	}
	assertSinkRecord(t, sink.delivered, records[0].ID)
	waitForModuleState(t, reader, records[0].ID, StateDelivered)

	shutdown()
	select {
	case <-service.Done():
	case <-time.After(time.Second):
		t.Fatal("outbox Service did not stop with ApplicationService")
	}
	if _, err = store.Get(context.Background(), records[0].ID); !errors.Is(err, ErrClosed) {
		t.Fatalf("store.Get() after shutdown = %v, want ErrClosed", err)
	}
}

func TestForFeatureRoutesMultipleDestinationsThroughOneWorkerPool(t *testing.T) {
	store := newServiceStore(t)
	featureModule := ForFeature(moduleTestStoreFeature(store), moduleServiceConfig())
	container, application := runOutboxContainer(t, featureModule)
	t.Cleanup(application.Shutdown)
	service, err := gioc.Get[*Service](container, OutboxServiceToken, featureModule)
	if err != nil {
		t.Fatal(err)
	}
	ordersSink := &moduleSink{delivered: make(chan Record, 1)}
	auditSink := &moduleSink{delivered: make(chan Record, 1)}
	if err = service.Register(ordersSink, DestinationsConfig{Destinations: []string{"orders.confirmed"}}); err != nil {
		t.Fatal(err)
	}
	if err = service.Register(auditSink, DestinationsConfig{Destinations: []string{"audit.created"}}); err != nil {
		t.Fatal(err)
	}
	if got := service.Destinations(); len(got) != 2 || got[0] != "audit.created" || got[1] != "orders.confirmed" {
		t.Fatalf("registered destinations = %v", got)
	}
	records, err := service.Append(t.Context(),
		NewRecord{ID: "orders-module-record", Destination: "orders.confirmed", MessageType: "order.confirmed", Payload: []byte("order")},
		NewRecord{ID: "audit-module-record", Destination: "audit.created", MessageType: "audit.created", Payload: []byte("audit")},
	)
	if err != nil {
		t.Fatal(err)
	}
	assertSinkRecord(t, ordersSink.delivered, records[0].ID)
	assertSinkRecord(t, auditSink.delivered, records[1].ID)
	waitForModuleState(t, service, records[0].ID, StateDelivered)
	waitForModuleState(t, service, records[1].ID, StateDelivered)
}

func TestForFeatureRejectsNilStore(t *testing.T) {
	feature := DefineStoreFeature("nil-test", func(token gioc.Token) gioc.IProvider {
		return gioc.FactoryProvider(token, gioc.NewFactory(nil, gioc.Singleton, func(gioc.Injections) (IStore, error) {
			return (*memoryTestStore)(nil), nil
		}), true)
	})
	featureModule := ForFeature(feature)
	applicationModule := react.ApplicationModuleFor(react.ApplicationConfig{Parent: context.Background()})
	container := gioc.NewContainer()
	if err := container.AddModules(applicationModule, testLoggerModule(), featureModule); err != nil {
		t.Fatal(err)
	}
	if err := container.Run(); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("Run() = %v, want ErrInvalidArgument", err)
	}
}

func moduleTestStoreFeature(store IStore) StoreFeature {
	return DefineStoreFeature("module-test", func(token gioc.Token) gioc.IProvider {
		return gioc.ValueProvider(token, store, true)
	})
}

func moduleServiceConfig() Config {
	config := DefaultConfig()
	config.WorkerCount = 2
	config.ClaimBatchSize = 2
	config.LeaseDuration = 500 * time.Millisecond
	config.RenewalEnabled = false
	config.DeliveryTimeout = 100 * time.Millisecond
	config.PollMinimumInterval = time.Millisecond
	config.PollMaximumInterval = 5 * time.Millisecond
	config.ShutdownTimeout = time.Second
	return config
}

func testLoggerModule() *gioc.Module {
	return gioc.NewModule("OutboxTestLogger").Global().Provide(
		react.Logger(author.Config{Level: author.FATAL}),
	)
}

func runOutboxContainer(t testing.TB, featureModule *gioc.Module) (*gioc.Container, *react.ApplicationService) {
	t.Helper()
	applicationModule := react.ApplicationModuleFor(react.ApplicationConfig{
		Parent: context.Background(), EnableShutDownHooks: true,
	})
	container := gioc.NewContainer()
	if err := container.AddModules(applicationModule, testLoggerModule(), featureModule); err != nil {
		t.Fatal(err)
	}
	if err := container.Run(); err != nil {
		t.Fatalf("Run() = %v", err)
	}
	application, err := gioc.Get[*react.ApplicationService](container, react.ApplicationContextServiceToken)
	if err != nil {
		t.Fatal(err)
	}
	return container, application
}

func waitForModuleState(t testing.TB, reader IReader, id ID, state State) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		record, err := reader.Get(ctx, id)
		if err == nil && record.State == state {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("record %q did not reach %q: last error %v", id, state, err)
		case <-ticker.C:
		}
	}
}

func assertSinkRecord(t testing.TB, records <-chan Record, id ID) {
	t.Helper()
	select {
	case record := <-records:
		if record.ID != id {
			t.Fatalf("sink record ID = %q, want %q", record.ID, id)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("sink did not receive record %q", id)
	}
}
