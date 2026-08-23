package outbox

import (
	"context"
	"fmt"

	"github.com/0x626f/gioc"
	"github.com/0x626f/react"

	reactredis "github.com/0x626f/react/redis"
)

// NewRedisStoreFromService constructs a store over React's application-owned Redis
// client. Closing the returned facade never closes the service client.
func NewRedisStoreFromService(ctx context.Context, service *reactredis.Service, config RedisConfig) (*RedisStore, error) {
	if service == nil || service.Client() == nil {
		return nil, fmt.Errorf("%w: Redis service is required", ErrInvalidArgument)
	}
	return NewRedisStore(ctx, service.Client(), config)
}

// RedisStoreProvider creates a singleton store at token from React's Redis service
// and application context. Use distinct tokens and namespaces for named
// outboxes. For standard module-managed wiring prefer ProvideRedisConfig with
// ForFeature; that path provides RedisStoreService and the service-owned
// worker pool.
func RedisStoreProvider(token gioc.Token, config RedisConfig) gioc.IProvider {
	return gioc.FactoryProvider(
		token,
		gioc.NewFactory(
			[]gioc.Token{reactredis.ServiceToken, react.ApplicationContextToken},
			gioc.Singleton,
			func(injections gioc.Injections) (*RedisStore, error) {
				injections.Require(reactredis.ServiceToken, react.ApplicationContextToken)
				service := gioc.MustResolve[*reactredis.Service](reactredis.ServiceToken, injections)
				ctx := gioc.MustResolve[context.Context](react.ApplicationContextToken, injections)
				return NewRedisStoreFromService(ctx, service, config)
			},
		),
		true,
	)
}
