package redis

import (
	"context"
	"fmt"

	"github.com/0x626f/gioc"
	"github.com/0x626f/react"
	"github.com/0x626f/react/outbox"
	reactredis "github.com/0x626f/react/redis"
)

// NewFromService constructs a store over React's application-owned Redis
// client. Closing the returned facade never closes the service client.
func NewFromService(ctx context.Context, service *reactredis.Service, config Config) (*Store, error) {
	if service == nil || service.Client() == nil {
		return nil, fmt.Errorf("%w: Redis service is required", outbox.ErrInvalidArgument)
	}
	return NewStore(ctx, service.Client(), config)
}

// StoreProvider creates a singleton store at token from React's Redis service
// and application context. Use distinct tokens and namespaces for named
// outboxes. For standard module-managed wiring prefer ProvideConfig with
// outbox.ForFeature; that path provides StoreService and the service-owned
// worker pool.
func StoreProvider(token gioc.Token, config Config) gioc.IProvider {
	return gioc.FactoryProvider(
		token,
		gioc.NewFactory(
			[]gioc.Token{reactredis.ServiceToken, react.ApplicationContextToken},
			gioc.Singleton,
			func(injections gioc.Injections) (*Store, error) {
				injections.Require(reactredis.ServiceToken, react.ApplicationContextToken)
				service := gioc.MustResolve[*reactredis.Service](reactredis.ServiceToken, injections)
				ctx := gioc.MustResolve[context.Context](react.ApplicationContextToken, injections)
				return NewFromService(ctx, service, config)
			},
		),
		true,
	)
}
