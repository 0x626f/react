package redis

import (
	"context"
	"fmt"

	"github.com/0x626f/gioc"
	"github.com/0x626f/react"
	"github.com/0x626f/react/outbox"
	reactredis "github.com/0x626f/react/redis"
)

const (
	// OutboxRedisConfigToken is an adapter-local alias of the package token
	// consumed by StoreService.
	OutboxRedisConfigToken gioc.Token = outbox.OutboxRedisConfigToken
)

// StoreServiceInjections lists the dependencies used by NewStoreService.
var StoreServiceInjections = []gioc.Token{
	reactredis.ServiceToken,
	react.ApplicationContextToken,
	OutboxRedisConfigToken,
	react.LoggerToken,
}

// Redis selects this adapter in outbox.ForFeature.
var Redis = outbox.DefineStoreFeature("redis", StoreServiceProvider)

// StoreService is the module-managed Redis Store facade. The application
// retains ownership of the injected Redis service and client.
type StoreService struct {
	*Store
	Logger react.ILogger
}

var _ outbox.IStore = (*StoreService)(nil)
var _ outbox.IStoreServiceInitializer = Config{}

// NewStoreService resolves React's Redis service, the application context, and
// Config from OutboxRedisConfigToken.
func NewStoreService(injections gioc.Injections) (*StoreService, error) {
	injections.Require(StoreServiceInjections...)
	logger := gioc.MustResolve[react.ILogger](react.LoggerToken, injections)
	service := gioc.MustResolve[*reactredis.Service](reactredis.ServiceToken, injections)
	ctx := gioc.MustResolve[context.Context](react.ApplicationContextToken, injections)
	config, err := resolveStoreServiceConfig(injections)
	if err != nil {
		logger.Error("initialize Redis outbox StoreService failed: %v", err)
		return nil, err
	}
	return newStoreService(ctx, service, config, logger)
}

func newStoreService(ctx context.Context, service *reactredis.Service, config Config, logger react.ILogger) (*StoreService, error) {
	store, err := NewFromService(ctx, service, config)
	if err != nil {
		logger.Error("initialize Redis outbox store failed: %v", err)
		return nil, err
	}
	for _, warning := range store.LastDurabilityReport().Warnings {
		logger.Warning("Redis outbox durability warning: %s", warning)
	}
	return &StoreService{Store: store, Logger: logger}, nil
}

// Close closes the adapter facade and logs an unexpected failure.
func (service *StoreService) Close() error {
	err := service.Store.Close()
	if err != nil {
		service.Logger.Error("close Redis outbox store failed: %v", err)
	}
	return err
}

// InitializeOutboxStoreService implements outbox.IStoreServiceInitializer for
// the root outbox.Redis shorthand feature.
func (config Config) InitializeOutboxStoreService(injections gioc.Injections) (outbox.IStore, error) {
	service := gioc.MustResolve[*reactredis.Service](reactredis.ServiceToken, injections)
	ctx := gioc.MustResolve[context.Context](react.ApplicationContextToken, injections)
	logger := gioc.MustResolve[react.ILogger](react.LoggerToken, injections)
	return newStoreService(ctx, service, config, logger)
}

func resolveStoreServiceConfig(injections gioc.Injections) (Config, error) {
	value := injections.MustResolve(OutboxRedisConfigToken)
	switch config := value.(type) {
	case Config:
		return config, nil
	case *Config:
		if config == nil {
			return Config{}, fmt.Errorf("%w: Redis outbox config is required", outbox.ErrInvalidArgument)
		}
		return *config, nil
	default:
		return Config{}, fmt.Errorf("%w: %s must contain redis.Config or *redis.Config", outbox.ErrInvalidArgument, OutboxRedisConfigToken)
	}
}

// StoreServiceProvider provides a singleton StoreService at token.
func StoreServiceProvider(token gioc.Token) gioc.IProvider {
	return gioc.FactoryProvider(
		token,
		gioc.NewFactory(StoreServiceInjections, gioc.Singleton, NewStoreService),
		true,
	)
}

// ProvideConfig returns a provider for OutboxRedisConfigToken.
func ProvideConfig(config Config) gioc.IProvider {
	copy := config
	return gioc.ValueProvider(OutboxRedisConfigToken, &copy, true)
}
