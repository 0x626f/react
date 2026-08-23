<div align="center">
    <pre style="background: none;">
 ███████████   ██████████   █████████     █████████  ███████████
░░███░░░░░███ ░░███░░░░░█  ███░░░░░███   ███░░░░░███░█░░░███░░░█
 ░███    ░███  ░███  █ ░  ░███    ░███  ███     ░░░ ░   ░███  ░ 
 ░██████████   ░██████    ░███████████ ░███             ░███    
 ░███░░░░░███  ░███░░█    ░███░░░░░███ ░███             ░███    
 ░███    ░███  ░███ ░   █ ░███    ░███ ░░███     ███    ░███    
 █████   █████ ██████████ █████   █████ ░░█████████     █████   
░░░░░   ░░░░░ ░░░░░░░░░░ ░░░░░   ░░░░░   ░░░░░░░░░     ░░░░░    
    </pre>
</div>

<div align="center">
    <h3>React</h3>
    <h6>Composable Go application modules built on github.com/0x626f/gioc</h6>
</div>

## Overview

`react` provides small application building blocks for Go services that use `gioc` dependency injection. It includes:

- application lifecycle and cancellation context management
- logger provider configuration using `github.com/0x626f/author`
- environment-backed config registration and derived config providers
- reusable base service structs for context, logger, and config injection
- optional Redis, PostgreSQL, and RabbitMQ modules
- a generic transactional outbox with inmemory, PostgreSQL, and Redis storage

## Installation

```bash
go get github.com/0x626f/react
```

Optional subpackages are imported only when needed:

```go
import (
    "github.com/0x626f/react"
    reactredis "github.com/0x626f/react/redis"
    reactrmq "github.com/0x626f/react/rmq"
    reactpostgres "github.com/0x626f/react/postgres"
)
```

## Quick Example

```go
package main

import (
    "context"
    "os"
    "syscall"

    "github.com/0x626f/author"
    "github.com/0x626f/gioc"
    "github.com/0x626f/react"
)

func main() {
    container := gioc.NewContainer()

    configModule := gioc.NewModule("Config").Global().Provide(
        gioc.ValueProvider(
            react.LoggerConfigToken,
            &react.LoggerModuleConfig{Level: author.INFO},
            true,
        ),
    )

    appModule := react.ApplicationModuleFor(react.ApplicationConfig{
        Parent:              context.Background(),
        Interruptions:       []os.Signal{os.Interrupt, syscall.SIGTERM},
        EnableShutDownHooks: true,
    })

    if err := container.AddModules(configModule, appModule, react.LoggerModule()); err != nil {
        panic(err)
    }
    if err := container.Run(); err != nil {
        panic(err)
    }

    app, err := gioc.Get[*react.ApplicationService](container, react.ApplicationContextServiceToken)
    if err != nil {
        panic(err)
    }
    app.Await()
}
```

## Config Module

Use `RegisterConfigModule` to load `.env` files and expose a typed app config. Providers can derive module-specific configs from the root config.

```go
package main

import (
    "github.com/0x626f/gioc"
    "github.com/0x626f/react"
    reactredis "github.com/0x626f/react/redis"
)

const AppConfigToken = gioc.Token("AppConfig")

type AppConfig struct {
    RedisURL string `env:"REDIS_URL"`
}

var ConfigModule = react.RegisterConfigModule[AppConfig](react.ConfigModuleParams[AppConfig]{
    AppConfigToken: AppConfigToken,
    LoadEnvs: []react.EnvEntry{
        {Path: ".env", Relative: true},
    },
    Providers: []react.ConfigProviders[AppConfig]{
        {
            Token: reactredis.ConfigToken,
            Provider: func(config *AppConfig) (any, error) {
                return &reactredis.Config{URL: config.RedisURL}, nil
            },
        },
    },
})
```

## Redis Example

```go
package main

import (
    "time"

    "github.com/0x626f/gioc"
    "github.com/0x626f/react"
    reactredis "github.com/0x626f/react/redis"
)

func resolveRedis(container *gioc.Container) (*reactredis.Service, error) {
    return gioc.Get[*reactredis.Service](container, reactredis.ServiceToken)
}

func cacheValue(service *reactredis.Service) error {
    if err := service.Set("example:key", "value", time.Minute); err != nil {
        return err
    }

    value, ok, err := service.Get("example:key")
    if err != nil || !ok {
        return err
    }

    _ = value
    return nil
}

func modules() []*gioc.Module {
    return []*gioc.Module{
        react.ApplicationModuleFor(react.ApplicationConfig{EnableShutDownHooks: true}),
        react.LoggerModule(),
        reactredis.Module,
    }
}
```

`reactredis.ConfigToken` must be provided with a `*redis.Config` containing `URL`, usually through `RegisterConfigModule`.

## RabbitMQ Example

```go
package main

import (
    "time"

    "github.com/0x626f/gioc"
    "github.com/0x626f/react/rmq"
    "github.com/rabbitmq/amqp091-go"
)

var rabbitMQConfig = &rmq.ModuleConfig{
    Host:        "localhost",
    Port:        5672,
    User:        "guest",
    Password:    "guest",
    VirtualHost: "operator",
}

func publish(container *gioc.Container) error {
    producer, err := gioc.Get[*rmq.ProducerService](container, rmq.ProducerServiceToken)
    if err != nil {
        return err
    }

    exchange := rmq.NewExchange("events").SetKind("direct").SetDurable(true)
    queue := rmq.NewQueue("events.created").SetDurable(true).SetBindings(
        rmq.Bind(exchange, "created")...,
    )

    service, err := gioc.Get[*rmq.Service](container, rmq.ServiceToken)
    if err != nil {
        return err
    }
    if _, err = service.CreateQueues(queue); err != nil {
        return err
    }

    producer.Bind(exchange)
    producer.WithTimeout(5 * time.Second)

    return producer.Produce(&rmq.Publication{
        Destination: "created",
        Message: amqp091.Publishing{
            ContentType: "application/json",
            Body:        []byte(`{"id":"example"}`),
        },
    })
}
```

Provide `rmq.ModuleConfigToken` with `ProvideModuleConfig` or a config derivation before adding `rmq.Module`.
`ModuleConfig.VirtualHost` is loaded from `RMQ_VIRTUAL_HOST`; leave it empty to use RabbitMQ's default `/` virtual host.
The example above connects to the `operator` virtual host.

## PostgreSQL Example

```go
package main

import (
    "github.com/0x626f/gioc"
    "github.com/0x626f/pgxext"
    reactpostgres "github.com/0x626f/react/postgres"
)

const UsersRepositoryToken = gioc.Token("UsersRepository")

type UsersRepository struct {
    ds *pgxext.DataSource
}

func NewUsersRepository(ds *pgxext.DataSource) *UsersRepository {
    return &UsersRepository{ds: ds}
}

var PostgresModule = reactpostgres.ForRepositories([]reactpostgres.RepositoryConstructor{
    reactpostgres.Repository(UsersRepositoryToken, NewUsersRepository),
})
```

Provide `reactpostgres.ConfigToken` with a `*postgres.Config` containing `URL` before adding the PostgreSQL module.

## Testing

```bash
go test ./...
```

## Transactional Outbox

The storage-independent outbox, adapters, worker pool, operational APIs, and
production guidance are documented in [outbox/README.md](outbox/README.md).

RabbitMQ integration tests are skipped unless `RMQ_TEST_URL` is set, for example:

```bash
RMQ_TEST_URL=amqp://guest:guest@localhost:5672 go test ./rmq -run Integration
```

## License

MIT. See [LICENSE](LICENSE).
