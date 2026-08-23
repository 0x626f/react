package postgres

import (
	"fmt"

	"github.com/0x626f/gioc"
	"github.com/0x626f/pgxext"
	"github.com/0x626f/react"
	"github.com/0x626f/react/outbox"
	reactpostgres "github.com/0x626f/react/postgres"
)

const (
	// OutboxPostgresConfigToken is an adapter-local alias of the package token
	// consumed by StoreService.
	OutboxPostgresConfigToken gioc.Token = outbox.OutboxPostgresConfigToken
)

// StoreServiceInjections lists the dependencies used by NewStoreService.
var StoreServiceInjections = []gioc.Token{
	reactpostgres.DataSourceToken,
	OutboxPostgresConfigToken,
	react.LoggerToken,
}

// Postgres selects this adapter in outbox.ForFeature.
var Postgres = outbox.DefineStoreFeature("postgres", StoreServiceProvider)

// StoreService is the module-managed PostgreSQL Store facade. The application
// retains ownership of the injected data source and its pool.
type StoreService struct {
	*Store
	Logger react.ILogger
}

var _ outbox.IStore = (*StoreService)(nil)
var _ outbox.IStoreServiceInitializer = Config{}

// NewStoreService resolves React's PostgreSQL data source and Config from
// OutboxPostgresConfigToken.
func NewStoreService(injections gioc.Injections) (*StoreService, error) {
	injections.Require(StoreServiceInjections...)
	logger := gioc.MustResolve[react.ILogger](react.LoggerToken, injections)
	dataSource := gioc.MustResolve[*pgxext.DataSource](reactpostgres.DataSourceToken, injections)
	config, err := resolveStoreServiceConfig(injections)
	if err != nil {
		logger.Error("initialize PostgreSQL outbox StoreService failed: %v", err)
		return nil, err
	}
	return newStoreService(dataSource, config, logger)
}

func newStoreService(dataSource *pgxext.DataSource, config Config, logger react.ILogger) (*StoreService, error) {
	store, err := NewFromDataSource(dataSource, config)
	if err != nil {
		logger.Error("initialize PostgreSQL outbox store failed: %v", err)
		return nil, err
	}
	return &StoreService{Store: store, Logger: logger}, nil
}

// Close closes the adapter facade and logs an unexpected failure.
func (service *StoreService) Close() error {
	err := service.Store.Close()
	if err != nil {
		service.Logger.Error("close PostgreSQL outbox store failed: %v", err)
	}
	return err
}

// InitializeOutboxStoreService implements outbox.IStoreServiceInitializer for
// the root outbox.Postgres shorthand feature.
func (config Config) InitializeOutboxStoreService(injections gioc.Injections) (outbox.IStore, error) {
	dataSource := gioc.MustResolve[*pgxext.DataSource](reactpostgres.DataSourceToken, injections)
	logger := gioc.MustResolve[react.ILogger](react.LoggerToken, injections)
	return newStoreService(dataSource, config, logger)
}

func resolveStoreServiceConfig(injections gioc.Injections) (Config, error) {
	value := injections.MustResolve(OutboxPostgresConfigToken)
	switch config := value.(type) {
	case Config:
		return config, nil
	case *Config:
		if config == nil {
			return Config{}, fmt.Errorf("%w: PostgreSQL outbox config is required", outbox.ErrInvalidArgument)
		}
		return *config, nil
	default:
		return Config{}, fmt.Errorf("%w: %s must contain postgres.Config or *postgres.Config", outbox.ErrInvalidArgument, OutboxPostgresConfigToken)
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

// ProvideConfig returns a provider for OutboxPostgresConfigToken.
func ProvideConfig(config Config) gioc.IProvider {
	copy := config
	return gioc.ValueProvider(OutboxPostgresConfigToken, &copy, true)
}
