package outbox_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/0x626f/author"
	"github.com/0x626f/gioc"
	"github.com/0x626f/react"
	"github.com/0x626f/react/outbox"
)

type moduleSink struct{ delivered chan outbox.Record }

func (sink *moduleSink) Deliver(ctx context.Context, record outbox.Record) error {
	select {
	case sink.delivered <- record.Clone():
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func TestForFeatureInitializesServiceAndProvidesCapabilities(t *testing.T) {
	storeConfig := outbox.DefaultInmemoryConfig()
	workerConfig := moduleServiceConfig()
	configModule := gioc.NewModule("OutboxModuleTestConfig").Provide(outbox.ProvideInmemoryConfig(storeConfig))
	featureModule := outbox.ForFeature(outbox.Inmemory, workerConfig).Import(configModule)
	container, application := runOutboxContainer(t, featureModule)
	var shutdownOnce sync.Once
	shutdown := func() { shutdownOnce.Do(application.Shutdown) }
	t.Cleanup(shutdown)

	service, err := gioc.Get[*outbox.Service](container, outbox.OutboxServiceToken, featureModule)
	if err != nil {
		t.Fatalf("resolve outbox Service = %v", err)
	}
	if service.ApplicationService != application {
		t.Fatal("outbox Service did not receive React's ApplicationService")
	}
	if service.Logger == nil {
		t.Fatal("outbox Service did not resolve ILogger")
	}
	storeService, err := gioc.Get[*outbox.InmemoryStoreService](container, outbox.OutboxStoreToken, featureModule)
	if err != nil {
		t.Fatalf("resolve inmemory StoreService = %v", err)
	}
	if service.IStore != storeService {
		t.Fatal("Service did not receive the module StoreService")
	}
	if storeService.Logger == nil {
		t.Fatal("StoreService did not resolve ILogger")
	}

	appender, err := gioc.Get[outbox.IAppender](container, outbox.OutboxAppenderToken, featureModule)
	if err != nil {
		t.Fatalf("resolve IAppender = %v", err)
	}
	reader, err := gioc.Get[outbox.IReader](container, outbox.OutboxReaderToken, featureModule)
	if err != nil {
		t.Fatalf("resolve IReader = %v", err)
	}
	deliveryStore, err := gioc.Get[outbox.IDeliveryStore](container, outbox.OutboxDeliveryStoreToken, featureModule)
	if err != nil {
		t.Fatalf("resolve IDeliveryStore = %v", err)
	}
	maintenance, err := gioc.Get[outbox.IMaintenanceStore](container, outbox.OutboxMaintenanceStoreToken, featureModule)
	if err != nil {
		t.Fatalf("resolve IMaintenanceStore = %v", err)
	}
	if appender != service || reader != service || deliveryStore != service || maintenance != service {
		t.Fatal("capability providers must expose the outbox Service")
	}

	sink := &moduleSink{delivered: make(chan outbox.Record, 1)}
	if err = service.Register(sink, outbox.DestinationsConfig{Destinations: []string{"orders.confirmed"}}); err != nil {
		t.Fatal(err)
	}
	records, err := appender.Append(t.Context(), outbox.NewRecord{
		ID: "module-record", Destination: "orders.confirmed",
		MessageType: "order.confirmed", Payload: []byte(`{"order_id":"42"}`),
	})
	if err != nil {
		t.Fatalf("Append() = %v", err)
	}
	assertSinkRecord(t, sink.delivered, records[0].ID)
	waitForModuleState(t, reader, records[0].ID, outbox.StateDelivered)

	shutdown()
	select {
	case <-service.Done():
	case <-time.After(time.Second):
		t.Fatal("outbox Service did not stop with ApplicationService")
	}
	if _, err = storeService.Get(context.Background(), records[0].ID); !errors.Is(err, outbox.ErrClosed) {
		t.Fatalf("StoreService.Get() after shutdown = %v, want ErrClosed", err)
	}
}

func TestForFeatureRoutesMultipleDestinationsThroughOneWorkerPool(t *testing.T) {
	configModule := gioc.NewModule("OutboxMultipleRoutesTestConfig").Provide(
		outbox.ProvideInmemoryConfig(outbox.DefaultInmemoryConfig()),
	)
	featureModule := outbox.ForFeature(outbox.Inmemory, moduleServiceConfig()).Import(configModule)
	container, application := runOutboxContainer(t, featureModule)
	t.Cleanup(application.Shutdown)
	service, err := gioc.Get[*outbox.Service](container, outbox.OutboxServiceToken, featureModule)
	if err != nil {
		t.Fatal(err)
	}
	ordersSink := &moduleSink{delivered: make(chan outbox.Record, 1)}
	auditSink := &moduleSink{delivered: make(chan outbox.Record, 1)}
	if err = service.Register(ordersSink, outbox.DestinationsConfig{Destinations: []string{"orders.confirmed"}}); err != nil {
		t.Fatal(err)
	}
	if err = service.Register(auditSink, outbox.DestinationsConfig{Destinations: []string{"audit.created"}}); err != nil {
		t.Fatal(err)
	}
	if got := service.Destinations(); len(got) != 2 || got[0] != "audit.created" || got[1] != "orders.confirmed" {
		t.Fatalf("registered destinations = %v", got)
	}
	records, err := service.Append(t.Context(),
		outbox.NewRecord{ID: "orders-module-record", Destination: "orders.confirmed", MessageType: "order.confirmed", Payload: []byte("order")},
		outbox.NewRecord{ID: "audit-module-record", Destination: "audit.created", MessageType: "audit.created", Payload: []byte("audit")},
	)
	if err != nil {
		t.Fatal(err)
	}
	assertSinkRecord(t, ordersSink.delivered, records[0].ID)
	assertSinkRecord(t, auditSink.delivered, records[1].ID)
	waitForModuleState(t, service, records[0].ID, outbox.StateDelivered)
	waitForModuleState(t, service, records[1].ID, outbox.StateDelivered)
}

func TestAdapterFeatureValueUsesStoreService(t *testing.T) {
	configModule := gioc.NewModule("OutboxAdapterFeatureTestConfig").Provide(
		gioc.ValueProvider(outbox.OutboxInmemoryConfigToken, outbox.DefaultInmemoryConfig(), true),
	)
	featureModule := outbox.ForFeature(outbox.Inmemory).Import(configModule)
	container, application := runOutboxContainer(t, featureModule)
	t.Cleanup(application.Shutdown)
	if _, err := gioc.Get[*outbox.InmemoryStoreService](container, outbox.OutboxStoreToken, featureModule); err != nil {
		t.Fatalf("resolve inmemory StoreService = %v", err)
	}
}

func TestForFeatureRejectsNilStoreConfig(t *testing.T) {
	configModule := gioc.NewModule("OutboxNilConfigTestConfig").Provide(
		gioc.ValueProvider(outbox.OutboxInmemoryConfigToken, (*outbox.InmemoryConfig)(nil), true),
	)
	featureModule := outbox.ForFeature(outbox.Inmemory).Import(configModule)
	applicationModule := react.ApplicationModuleFor(react.ApplicationConfig{Parent: context.Background()})
	container := gioc.NewContainer()
	if err := container.AddModules(applicationModule, testLoggerModule(), featureModule); err != nil {
		t.Fatal(err)
	}
	if err := container.Run(); !errors.Is(err, outbox.ErrInvalidArgument) {
		t.Fatalf("Run() = %v, want ErrInvalidArgument", err)
	}
}

func moduleServiceConfig() outbox.Config {
	config := outbox.DefaultConfig()
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

func waitForModuleState(t testing.TB, reader outbox.IReader, id outbox.ID, state outbox.State) {
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

func assertSinkRecord(t testing.TB, records <-chan outbox.Record, id outbox.ID) {
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
