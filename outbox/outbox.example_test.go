package outbox_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/0x626f/author"
	"github.com/0x626f/gioc"
	"github.com/0x626f/react"
	"github.com/0x626f/react/outbox"

	"github.com/jackc/pgx/v5/pgxpool"
	goredis "github.com/redis/go-redis/v9"
)

func Example_inmemory() {
	store, err := outbox.NewInmemoryStore(outbox.DefaultInmemoryConfig())
	if err != nil {
		panic(err)
	}
	defer store.Close()
	records, err := store.Append(context.Background(), outbox.NewRecord{
		ID: "order-42-created", Destination: "orders", MessageType: "order.created",
		IdempotencyKey: "order-42-created", Payload: []byte(`{"order_id":"42"}`),
		MaxAttempts: 8,
	})
	if err != nil {
		panic(err)
	}
	fmt.Println(records[0].State)
	// Output: pending
}

func Example_postgresStandalone() {
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, os.Getenv("POSTGRES_URL"))
	if err != nil {
		return
	}
	defer pool.Close()
	config := outbox.DefaultPostgresConfig()
	config.Namespace = "orders"
	store, err := outbox.NewPostgresStore(pool, config)
	if err != nil {
		return
	}
	if err = store.Migrate(ctx); err != nil {
		return
	}
	_, _ = store.Append(ctx, exampleRecord("standalone-postgres"))
}

func Example_postgresDomainTransaction() {
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, os.Getenv("POSTGRES_URL"))
	if err != nil {
		return
	}
	defer pool.Close()
	store, err := outbox.NewPostgresStore(pool, outbox.DefaultPostgresConfig())
	if err != nil {
		return
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		return
	}
	defer tx.Rollback(context.Background())
	if _, err = tx.Exec(ctx, `UPDATE orders SET state='confirmed' WHERE id=$1`, "42"); err != nil {
		return
	}
	if _, err = store.Bind(tx).Append(ctx, exampleRecord("atomic-postgres")); err != nil {
		return
	}
	_ = tx.Commit(ctx)
}

func Example_redisStandalone() {
	options, err := goredis.ParseURL(os.Getenv("REDIS_URL"))
	if err != nil {
		return
	}
	client := goredis.NewClient(options)
	defer client.Close()
	config := outbox.DefaultRedisConfig()
	config.Namespace = "orders"
	config.DurabilityMode = outbox.RedisDurabilityRequireAOF
	store, err := outbox.NewRedisStore(context.Background(), client, config)
	if err != nil {
		return
	}
	_, _ = store.Append(context.Background(), exampleRecord("standalone-redis"))
}

func Example_redisDomainComposition() {
	options, err := goredis.ParseURL(os.Getenv("REDIS_URL"))
	if err != nil {
		return
	}
	client := goredis.NewClient(options)
	defer client.Close()
	config := outbox.DefaultRedisConfig()
	config.Namespace = "orders"
	store, err := outbox.NewRedisStore(context.Background(), client, config)
	if err != nil {
		return
	}
	domainKey := "react:outbox:{orders}:domain:order-42"
	_, _ = store.Compose(context.Background(), outbox.RedisCompositionRequest{
		Append:     outbox.AppendRequest{Records: []outbox.NewRecord{exampleRecord("composed-redis")}, DuplicateMode: outbox.RejectDuplicate},
		DomainKeys: []string{domainKey}, DomainArguments: []any{"pending", "confirmed"},
		ValidateLua: `
local value = redis.call('GET', KEYS[DOMAIN_KEY_OFFSET+1])
if value ~= ARGV[DOMAIN_ARG_OFFSET+1] then return {-3} end`,
		ApplyLua: `redis.call('SET', KEYS[DOMAIN_KEY_OFFSET+1], ARGV[DOMAIN_ARG_OFFSET+2])`,
	})
}

func Example_queryCancelAndReplay() {
	store, err := outbox.NewInmemoryStore(outbox.DefaultInmemoryConfig())
	if err != nil {
		return
	}
	defer store.Close()
	ctx := context.Background()
	_, _ = store.Append(ctx, exampleRecord("cancel-example"))
	page, _ := store.Find(ctx, outbox.Query{States: []outbox.State{outbox.StatePending}, Limit: 50})
	if len(page.Records) > 0 {
		_ = store.Cancel(ctx, page.Records[0].ID, "operator request")
	}
	// Dead records are replayed explicitly:
	_ = store.Requeue(ctx, "some-dead-record", outbox.RequeueOptions{AvailableAt: time.Now(), ResetAttempts: true})
}

func ExampleService_Register() {
	applicationModule := react.ApplicationModuleFor(react.ApplicationConfig{
		Parent: context.Background(), EnableShutDownHooks: true,
	})
	loggerModule := gioc.NewModule("ExampleLogger").Global().Provide(
		react.Logger(author.Config{Level: author.FATAL}),
	)
	configModule := gioc.NewModule("ExampleOutboxConfig").Provide(
		outbox.ProvideInmemoryConfig(outbox.DefaultInmemoryConfig()),
	)
	workerConfig := outbox.DefaultConfig()
	featureModule := outbox.ForFeature(outbox.Inmemory, workerConfig).Import(configModule)
	container := gioc.NewContainer()
	_ = container.AddModules(applicationModule, loggerModule, featureModule)
	_ = container.Run()
	application, _ := gioc.Get[*react.ApplicationService](container, react.ApplicationContextServiceToken)
	defer application.Shutdown()
	service, _ := gioc.Get[*outbox.Service](container, outbox.OutboxServiceToken, featureModule)
	delivered := make(chan outbox.Record, 1)
	_ = service.Register(outbox.SinkFunc(func(_ context.Context, record outbox.Record) error {
		delivered <- record
		return nil
	}), outbox.DestinationsConfig{Destinations: []string{"orders"}, Concurrency: 2})
	_, _ = service.Append(context.Background(), exampleRecord("service-example"))
	fmt.Println((<-delivered).ID)
	// Output: service-example
}

func Example_rabbitMQPublisherConfirms() {
	// IConfirmedPublisher must enable publisher confirms, use a stable message
	// ID, and resolve only after ack/nack plus mandatory-return handling.
	var publisher IConfirmedPublisher
	var sink outbox.ISink = rabbitConfirmedSink{publisher: publisher}
	_ = sink
}

type exampleSink struct{}

func (exampleSink) Deliver(context.Context, outbox.Record) error { return nil }

// IConfirmedPublisher publishes a message and waits for its broker outcome.
type IConfirmedPublisher interface {
	PublishAndConfirm(ctx context.Context, destination string, messageID string, headers map[string]string, payload []byte) (routed bool, err error)
}

type rabbitConfirmedSink struct{ publisher IConfirmedPublisher }

func (sink rabbitConfirmedSink) Deliver(ctx context.Context, record outbox.Record) error {
	if sink.publisher == nil {
		return errors.New("publisher unavailable")
	}
	routed, err := sink.publisher.PublishAndConfirm(ctx, record.Destination, string(record.ID), record.Headers, record.Payload)
	if err != nil {
		return err
	} // pre-confirm connection loss remains retryable/ambiguous
	if !routed {
		return &outbox.TerminalError{Err: errors.New("message was returned as unroutable")}
	}
	return nil
}

func exampleRecord(id outbox.ID) outbox.NewRecord {
	return outbox.NewRecord{ID: id, Destination: "orders", MessageType: "order.confirmed", Payload: []byte(`{"order_id":"42"}`), MaxAttempts: 8}
}
