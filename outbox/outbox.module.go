package outbox

import (
	"fmt"
	"reflect"

	"github.com/0x626f/gioc"
	"github.com/0x626f/react"
)

// StoreProviderFactory creates a fresh provider for an adapter StoreService at
// the requested aggregate-store token. Applications normally select one of the
// first-party feature values.
type StoreProviderFactory func(token gioc.Token) gioc.IProvider

// IStoreServiceInitializer lets the root package expose first-party shorthand
// features without importing adapter packages and creating import cycles.
type IStoreServiceInitializer interface {
	InitializeOutboxStoreService(injections gioc.Injections) (IStore, error)
}

// StoreFeature is an immutable storage adapter description selected by
// ForFeature.
type StoreFeature struct {
	name     string
	provider StoreProviderFactory
}

// DefineStoreFeature creates a feature descriptor without global mutable
// registration or init side effects.
func DefineStoreFeature(name string, provider StoreProviderFactory) StoreFeature {
	if err := ValidateID(ID(name), DefaultLimits()); err != nil {
		return StoreFeature{name: name}
	}
	return StoreFeature{name: name, provider: provider}
}

// Name returns the adapter's stable feature name.
func (feature StoreFeature) Name() string { return feature.name }

var (
	// Postgres selects outbox/postgres. Provide *postgres.Config at
	// OutboxPostgresConfigToken and import React's PostgreSQL module.
	Postgres = DefineStoreFeature("postgres", configuredStoreServiceProvider(
		OutboxPostgresConfigToken,
		postgresDataSourceToken,
	))
	// Redis selects outbox/redis. Provide *redis.Config at
	// OutboxRedisConfigToken and import React's Redis module.
	Redis = DefineStoreFeature("redis", configuredStoreServiceProvider(
		OutboxRedisConfigToken,
		redisServiceToken,
		applicationContextToken,
	))
	// Inmemory selects outbox/inmemory. It is process-local and non-durable.
	// Provide *inmemory.Config at OutboxInmemoryConfigToken.
	Inmemory = DefineStoreFeature("inmemory", configuredStoreServiceProvider(
		OutboxInmemoryConfigToken,
	))
)

// ForFeature creates one outbox module backed by storage. The optional Config
// controls its single service-level worker pool; omitting it uses DefaultConfig.
// Sinks and routes are registered through Service.Register.
func ForFeature(storage StoreFeature, configs ...Config) *gioc.Module {
	moduleName := string(OutboxModuleToken) + ":" + storage.name
	if storage.name == "" {
		moduleName += "invalid"
	}
	module := gioc.NewModule(gioc.NewToken(moduleName))

	if storage.provider == nil {
		module.Provide(invalidStoreFeatureProvider(storage))
	} else {
		module.Provide(storage.provider(OutboxStoreToken))
	}
	module.Provide(serviceConfigProvider(configs))
	module.Provide(outboxServiceProvider())
	module.Provide(ServiceCapabilityProviders(ModuleTokens())...)
	return module
}

func serviceConfigProvider(configs []Config) gioc.IProvider {
	copied := append([]Config(nil), configs...)
	return gioc.FactoryProvider(
		OutboxConfigToken,
		gioc.NewFactory(
			nil,
			gioc.Singleton,
			func(gioc.Injections) (*Config, error) {
				if len(copied) > 1 {
					return nil, invalid("config", "ForFeature accepts at most one service config")
				}
				config := DefaultConfig()
				if len(copied) == 1 {
					config = copied[0]
				}
				return &config, nil
			},
		),
		true,
	)
}

func invalidStoreFeatureProvider(feature StoreFeature) gioc.IProvider {
	return gioc.FactoryProvider(
		OutboxStoreToken,
		gioc.NewFactory(
			nil,
			gioc.Singleton,
			func(gioc.Injections) (IStore, error) {
				return nil, fmt.Errorf("%w: outbox storage feature %q has no store service provider", ErrInvalidArgument, feature.name)
			},
		),
		true,
	)
}

func configuredStoreServiceProvider(configToken gioc.Token, dependencies ...gioc.Token) StoreProviderFactory {
	injects := make([]gioc.Token, 1, len(dependencies)+2)
	injects[0] = configToken
	injects = append(injects, dependencies...)
	injects = append(injects, react.LoggerToken)
	return func(token gioc.Token) gioc.IProvider {
		featureInjects := append([]gioc.Token(nil), injects...)
		return gioc.FactoryProvider(
			token,
			gioc.NewFactory(
				featureInjects,
				gioc.Singleton,
				func(injections gioc.Injections) (IStore, error) {
					initializer := gioc.MustResolve[IStoreServiceInitializer](configToken, injections)
					if isNilValue(initializer) {
						return nil, fmt.Errorf("%w: outbox config at %q is nil", ErrInvalidArgument, configToken)
					}
					store, err := initializer.InitializeOutboxStoreService(injections)
					if err != nil {
						return nil, err
					}
					if isNilValue(store) {
						return nil, fmt.Errorf("%w: outbox config at %q returned a nil store service", ErrInvalidArgument, configToken)
					}
					return store, nil
				},
			),
			true,
		)
	}
}

func isNilValue(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
}
