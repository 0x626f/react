package outbox

import (
	"fmt"
	"reflect"

	"github.com/0x626f/gioc"
)

// StoreProviderFactory creates a fresh provider for an adapter store service at
// the requested aggregate-store token. Applications normally select one of the
// first-party feature values.
type StoreProviderFactory func(token gioc.Token) gioc.IProvider

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
	// Postgres selects PostgresStoreService. Provide PostgresConfig at
	// OutboxPostgresConfigToken and import React's PostgreSQL module.
	Postgres = DefineStoreFeature("postgres", PostgresStoreServiceProvider)
	// Redis selects RedisStoreService. Provide RedisConfig at
	// OutboxRedisConfigToken and import React's Redis module.
	Redis = DefineStoreFeature("redis", RedisStoreServiceProvider)
	// Inmemory selects the process-local, non-durable InmemoryStoreService.
	Inmemory = DefineStoreFeature("inmemory", InmemoryStoreServiceProvider)
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
