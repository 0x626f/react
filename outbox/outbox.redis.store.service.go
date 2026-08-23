package outbox

import (
	"context"
	"fmt"

	"github.com/0x626f/gioc"
	"github.com/0x626f/react"
	reactredis "github.com/0x626f/react/redis"
)

// RedisStoreServiceInjections lists the dependencies used by NewRedisStoreService.
var RedisStoreServiceInjections = []gioc.Token{
	reactredis.ServiceToken,
	react.ApplicationContextToken,
	OutboxRedisConfigToken,
	react.LoggerToken,
}

// RedisStoreService is the module-managed Redis store facade. The application
// retains ownership of the injected Redis service and client.
type RedisStoreService struct {
	*RedisStore
	Logger react.ILogger
}

var _ IStore = (*RedisStoreService)(nil)

// NewRedisStoreService resolves React's Redis service, the application context, and
// RedisConfig from OutboxRedisConfigToken.
func NewRedisStoreService(injections gioc.Injections) (*RedisStoreService, error) {
	injections.Require(RedisStoreServiceInjections...)
	logger := gioc.MustResolve[react.ILogger](react.LoggerToken, injections)
	service := gioc.MustResolve[*reactredis.Service](reactredis.ServiceToken, injections)
	ctx := gioc.MustResolve[context.Context](react.ApplicationContextToken, injections)
	config, err := resolveRedisStoreServiceConfig(injections)
	if err != nil {
		logger.Error("initialize Redis outbox RedisStoreService failed: %v", err)
		return nil, err
	}
	return newRedisStoreService(ctx, service, config, logger)
}

func newRedisStoreService(ctx context.Context, service *reactredis.Service, config RedisConfig, logger react.ILogger) (*RedisStoreService, error) {
	store, err := NewRedisStoreFromService(ctx, service, config)
	if err != nil {
		logger.Error("initialize Redis outbox store failed: %v", err)
		return nil, err
	}
	for _, warning := range store.LastDurabilityReport().Warnings {
		logger.Warning("Redis outbox durability warning: %s", warning)
	}
	return &RedisStoreService{RedisStore: store, Logger: logger}, nil
}

// Close closes the adapter facade and logs an unexpected failure.
func (service *RedisStoreService) Close() error {
	err := service.RedisStore.Close()
	if err != nil {
		service.Logger.Error("close Redis outbox store failed: %v", err)
	}
	return err
}

func resolveRedisStoreServiceConfig(injections gioc.Injections) (RedisConfig, error) {
	value := injections.MustResolve(OutboxRedisConfigToken)
	switch config := value.(type) {
	case RedisConfig:
		return config, nil
	case *RedisConfig:
		if config == nil {
			return RedisConfig{}, fmt.Errorf("%w: Redis outbox config is required", ErrInvalidArgument)
		}
		return *config, nil
	default:
		return RedisConfig{}, fmt.Errorf("%w: %s must contain outbox.RedisConfig or *outbox.RedisConfig", ErrInvalidArgument, OutboxRedisConfigToken)
	}
}

// RedisStoreServiceProvider provides a singleton RedisStoreService at token.
func RedisStoreServiceProvider(token gioc.Token) gioc.IProvider {
	return gioc.FactoryProvider(
		token,
		gioc.NewFactory(RedisStoreServiceInjections, gioc.Singleton, NewRedisStoreService),
		true,
	)
}

// ProvideRedisConfig returns a provider for OutboxRedisConfigToken.
func ProvideRedisConfig(config RedisConfig) gioc.IProvider {
	copy := config
	return gioc.ValueProvider(OutboxRedisConfigToken, &copy, true)
}
