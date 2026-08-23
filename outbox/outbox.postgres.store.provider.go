package outbox

import (
	"fmt"

	"github.com/0x626f/gioc"
	"github.com/0x626f/pgxext"

	reactpostgres "github.com/0x626f/react/postgres"
)

// NewPostgresStoreFromDataSource constructs a store over React's application-owned
// PostgreSQL pool. Closing the returned facade never closes the data source.
func NewPostgresStoreFromDataSource(dataSource *pgxext.DataSource, config PostgresConfig) (*PostgresStore, error) {
	if dataSource == nil || dataSource.Pool == nil {
		return nil, fmt.Errorf("%w: PostgreSQL data source is required", ErrInvalidArgument)
	}
	return NewPostgresStore(dataSource.Pool, config)
}

// PostgresStoreProvider creates a singleton store at token from React's PostgreSQL
// DataSourceToken. Use distinct tokens for independently named outboxes. For
// standard module-managed wiring prefer ProvidePostgresConfig with ForFeature;
// that path provides PostgresStoreService and the service-owned worker pool.
func PostgresStoreProvider(token gioc.Token, config PostgresConfig) gioc.IProvider {
	return gioc.FactoryProvider(
		token,
		gioc.NewFactory(
			[]gioc.Token{reactpostgres.DataSourceToken},
			gioc.Singleton,
			func(injections gioc.Injections) (*PostgresStore, error) {
				injections.Require(reactpostgres.DataSourceToken)
				dataSource := gioc.MustResolve[*pgxext.DataSource](reactpostgres.DataSourceToken, injections)
				return NewPostgresStoreFromDataSource(dataSource, config)
			},
		),
		true,
	)
}
