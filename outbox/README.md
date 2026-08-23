# Transactional outbox

This package provides a storage-independent, at-least-once transactional
outbox. `Service` is the single application boundary: it exposes storage
capabilities, owns destination routing, polls the delivery store, runs one
bounded worker pool, renews leases, settles outcomes, and participates in
React shutdown.

The implementation deliberately has no instrumentation hooks on append,
claim, delivery, retry, or query paths. Services receive `react.ILogger` from
`react.LoggerToken` and log operational failures only.

## Consumer API

Select one adapter and optionally provide the worker-pool configuration:

```go
workerConfig := outbox.DefaultConfig()
workerConfig.WorkerCount = 8

outboxModule := outbox.ForFeature(outbox.Postgres, workerConfig).
    Import(postgresConfigModule)
```

The selected adapter configuration remains a normal GIOC value:

```go
storeConfig := outbox.DefaultPostgresConfig()
storeConfig.Namespace = "orders"

postgresConfigModule := gioc.NewModule("OrdersOutboxConfig").Provide(
    outbox.ProvidePostgresConfig(storeConfig),
)
```

Add React's application, logger, and adapter infrastructure modules before
running the container. Then resolve the service and register each sink:

```go
service, err := gioc.Get[*outbox.Service](
    container,
    outbox.OutboxServiceToken,
    outboxModule,
)
if err != nil {
    return err
}

err = service.Register(rabbitSink, outbox.DestinationsConfig{
    Destinations: []string{"orders.confirmed", "orders.cancelled"},
    Concurrency:  4,
})
if err != nil {
    return err
}
```

`Register` copies the configuration and atomically installs all routes. A
destination can be registered only once. `Concurrency` bounds all deliveries
through that registration and defaults to the global worker count. The global
`Config.WorkerCount` remains the hard process-wide bound.

Records for an unregistered destination remain pending. Registering a route
wakes the poller, so consumers may register after container startup without a
polling delay.

For a small consumer, adapt a function directly with `outbox.SinkFunc` instead
of declaring a separate sink type.

The service itself implements `IStore`, so producers can append directly:

```go
records, err := service.Append(ctx, outbox.NewRecord{
    ID:             "order-42-confirmed",
    Destination:    "orders.confirmed",
    MessageType:    "order.confirmed",
    AggregateType:  "order",
    AggregateID:    "42",
    OrderingKey:    "42",
    IdempotencyKey: "order.confirmed:42",
    Payload:        payload,
    MaxAttempts:    12,
})
```

Prefer the narrow tokens when a consumer needs less authority:

- `OutboxAppenderToken` provides `IAppender`.
- `OutboxReaderToken` provides `IReader`.
- `OutboxDeliveryStoreToken` provides `IDeliveryStore`.
- `OutboxMaintenanceStoreToken` provides `IMaintenanceStore`.

All capability providers resolve `Service`, not the adapter directly. The
adapter aggregate remains available at `OutboxStoreToken` for adapter-specific
operations such as PostgreSQL transaction binding and migrations.

## Atomicity boundary

A transactional outbox is atomic only when the domain mutation and outbox
append commit in the same transactional resource. The package cannot create
atomicity across unrelated databases.

For PostgreSQL, bind the outbox store to the caller-owned transaction:

```go
tx, err := database.Begin(ctx)
if err != nil {
    return err
}
defer tx.Rollback(context.Background())

if _, err = tx.Exec(ctx,
    `UPDATE orders SET state='confirmed' WHERE id=$1`, orderID,
); err != nil {
    return err
}

if _, err = postgresStore.Bind(tx).Append(ctx, record); err != nil {
    return err
}
return tx.Commit(ctx)
```

The caller owns the outer transaction. `Bind` uses a savepoint for atomic
batch validation and never commits or rolls back the outer transaction.

Redis composition is possible only when domain keys and outbox keys share the
same Redis Cluster hash slot. See `outbox.RedisCompositionRequest`. PostgreSQL domain
state plus a Redis outbox is not a transactional outbox.

## Delivery model

The portable state machine is:

```text
pending   --claim--------------------------> leased
leased    --acknowledge--------------------> delivered
leased    --retry or release---------------> pending
leased    --terminal/exhausted failure-----> dead
leased    --expired lease recovery---------> pending or dead
pending   --cancel--------------------------> cancelled
dead      --requeue-------------------------> pending
pending   --reschedule----------------------> pending
```

Claims are fenced by record ID, owner, unguessable lease token, version, and
storage-authoritative expiry. Stale settlement receives `ErrLeaseLost`.

Delivery is at least once, never exactly once. A sink can publish successfully
and the process can stop before `Acknowledge` is persisted. The record becomes
eligible again after lease recovery. Consumers must deduplicate by stable
record ID or implement an idempotent domain operation.

`ISink.Deliver` must return nil only after the downstream system has reliably
accepted the message. For RabbitMQ that normally means a persistent mandatory
publish followed by a positive publisher confirmation and no returned message.

Errors are classified as:

- `OutcomeRetryable`: schedule another bounded attempt.
- `OutcomeTerminal`: move directly to `dead`.
- `OutcomeAmbiguous`: retry because loss is worse than a possible duplicate.

`TerminalError` selects terminal behavior with the default classifier. A
custom `IErrorClassifier` or `IRetryPolicy` can be provided in `Config`.

## Worker pool and shutdown

One poller claims only currently registered destinations. Large routing tables
are rotated through bounded claim windows so every first-party adapter receives
portable requests. Claims never hold a database transaction open while a sink
performs network I/O.

Workers enforce delivery timeouts, renew long-running leases before their
threshold, and settle with a bounded background context. Shutdown stops new
claims first and allows current deliveries to finish. If the deadline is
reached, delivery contexts are cancelled and still-current leases are released
using their fences.

React registers this work as a pre-shutdown hook. The adapter facade closes in
the normal shutdown tier after the worker pool. PostgreSQL pools, Redis clients,
and broker connections remain owned by their infrastructure modules.

## Storage adapters

### PostgreSQL

`outbox.PostgresStore` provides durable storage with `FOR UPDATE SKIP LOCKED`,
fenced mutations, indexed operational queries, transaction binding, and
explicit migrations. Apply `PostgresStore.Migrations()` or
`PostgresStore.Migrate(ctx)` during deployment before the service begins
processing records. The application owns the pgx pool.

### Redis

`outbox.RedisStore` uses bounded atomic Lua scripts and same-slot keys. Redis
is authoritative storage, not a cache. Production use requires a deliberate
AOF, replication, backup, failover, and `noeviction` posture. Startup
durability checks can warn or fail according to `DurabilityMode`. The
application owns the Redis client.

## Logging

`react.ILogger` is an alias of `author.ILogger`. `Service` and each adapter
store service resolve it from `react.LoggerToken`; the standard
`react.LoggerModule()` supplies the implementation. Logging is limited to
operational failures and durability warnings; payloads and headers are never
logged by first-party code.

## Validation

Run unit, lifecycle, routing, lease, retry, and adapter contract tests:

```sh
go test ./outbox
go test -race ./outbox
```

PostgreSQL and Redis integration tests use `OUTBOX_POSTGRES_TEST_URL` and
`OUTBOX_REDIS_TEST_URL` when those variables are present.
