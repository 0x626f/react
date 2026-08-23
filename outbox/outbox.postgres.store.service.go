package outbox

import (
	"fmt"

	"github.com/0x626f/gioc"
	"github.com/0x626f/pgxext"
	"github.com/0x626f/react"
	reactpostgres "github.com/0x626f/react/postgres"
)

// PostgresStoreServiceInjections lists the dependencies used by NewPostgresStoreService.
var PostgresStoreServiceInjections = []gioc.Token{
	reactpostgres.DataSourceToken,
	OutboxPostgresConfigToken,
	react.LoggerToken,
}

// PostgresStoreService is the module-managed PostgreSQL store facade. The
// application retains ownership of the injected data source and its pool.
type PostgresStoreService struct {
	*PostgresStore
	Logger react.ILogger
}

var _ IStore = (*PostgresStoreService)(nil)

// NewPostgresStoreService resolves React's PostgreSQL data source and PostgresConfig from
// OutboxPostgresConfigToken.
func NewPostgresStoreService(injections gioc.Injections) (*PostgresStoreService, error) {
	injections.Require(PostgresStoreServiceInjections...)
	logger := gioc.MustResolve[react.ILogger](react.LoggerToken, injections)
	dataSource := gioc.MustResolve[*pgxext.DataSource](reactpostgres.DataSourceToken, injections)
	config, err := resolvePostgresStoreServiceConfig(injections)
	if err != nil {
		logger.Error("initialize PostgreSQL outbox PostgresStoreService failed: %v", err)
		return nil, err
	}
	return newPostgresStoreService(dataSource, config, logger)
}

func newPostgresStoreService(dataSource *pgxext.DataSource, config PostgresConfig, logger react.ILogger) (*PostgresStoreService, error) {
	store, err := NewPostgresStoreFromDataSource(dataSource, config)
	if err != nil {
		logger.Error("initialize PostgreSQL outbox store failed: %v", err)
		return nil, err
	}
	return &PostgresStoreService{PostgresStore: store, Logger: logger}, nil
}

// Close closes the adapter facade and logs an unexpected failure.
func (service *PostgresStoreService) Close() error {
	err := service.PostgresStore.Close()
	if err != nil {
		service.Logger.Error("close PostgreSQL outbox store failed: %v", err)
	}
	return err
}

func resolvePostgresStoreServiceConfig(injections gioc.Injections) (PostgresConfig, error) {
	value := injections.MustResolve(OutboxPostgresConfigToken)
	switch config := value.(type) {
	case PostgresConfig:
		return config, nil
	case *PostgresConfig:
		if config == nil {
			return PostgresConfig{}, fmt.Errorf("%w: PostgreSQL outbox config is required", ErrInvalidArgument)
		}
		return *config, nil
	default:
		return PostgresConfig{}, fmt.Errorf("%w: %s must contain outbox.PostgresConfig or *outbox.PostgresConfig", ErrInvalidArgument, OutboxPostgresConfigToken)
	}
}

// PostgresStoreServiceProvider provides a singleton PostgresStoreService at token.
func PostgresStoreServiceProvider(token gioc.Token) gioc.IProvider {
	return gioc.FactoryProvider(
		token,
		gioc.NewFactory(PostgresStoreServiceInjections, gioc.Singleton, NewPostgresStoreService),
		true,
	)
}

// ProvidePostgresConfig returns a provider for OutboxPostgresConfigToken.
func ProvidePostgresConfig(config PostgresConfig) gioc.IProvider {
	copy := config
	return gioc.ValueProvider(OutboxPostgresConfigToken, &copy, true)
}
