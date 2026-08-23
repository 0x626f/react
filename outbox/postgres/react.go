package postgres

import (
	"fmt"

	"github.com/0x626f/gioc"
	"github.com/0x626f/pgxext"
	"github.com/0x626f/react/outbox"
	reactpostgres "github.com/0x626f/react/postgres"
)

// NewFromDataSource constructs a store over React's application-owned
// PostgreSQL pool. Closing the returned facade never closes the data source.
func NewFromDataSource(dataSource *pgxext.DataSource, config Config) (*Store, error) {
	if dataSource == nil || dataSource.Pool == nil {
		return nil, fmt.Errorf("%w: PostgreSQL data source is required", outbox.ErrInvalidArgument)
	}
	return NewStore(dataSource.Pool, config)
}

// StoreProvider creates a singleton store at token from React's PostgreSQL
// DataSourceToken. Use distinct tokens for independently named outboxes. For
// standard module-managed wiring prefer ProvideConfig with outbox.ForFeature;
// that path provides StoreService and the service-owned worker pool.
func StoreProvider(token gioc.Token, config Config) gioc.IProvider {
	return gioc.FactoryProvider(
		token,
		gioc.NewFactory(
			[]gioc.Token{reactpostgres.DataSourceToken},
			gioc.Singleton,
			func(injections gioc.Injections) (*Store, error) {
				injections.Require(reactpostgres.DataSourceToken)
				dataSource := gioc.MustResolve[*pgxext.DataSource](reactpostgres.DataSourceToken, injections)
				return NewFromDataSource(dataSource, config)
			},
		),
		true,
	)
}
