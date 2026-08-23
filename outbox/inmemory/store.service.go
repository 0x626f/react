package inmemory

import (
	"fmt"

	"github.com/0x626f/gioc"
	"github.com/0x626f/react"
	"github.com/0x626f/react/outbox"
)

const (
	// OutboxInmemoryConfigToken is an adapter-local alias of the package token
	// consumed by StoreService.
	OutboxInmemoryConfigToken gioc.Token = outbox.OutboxInmemoryConfigToken
)

// StoreServiceInjections lists the dependencies used by NewStoreService.
var StoreServiceInjections = []gioc.Token{OutboxInmemoryConfigToken, react.LoggerToken}

// Inmemory selects this adapter in outbox.ForFeature.
var Inmemory = outbox.DefineStoreFeature("inmemory", StoreServiceProvider)

// StoreService is the module-managed process-local Store facade. Closing it
// never owns or closes an external resource.
type StoreService struct {
	*Store
	Logger react.ILogger
}

var _ outbox.IStore = (*StoreService)(nil)
var _ outbox.IStoreServiceInitializer = Config{}

// NewStoreService resolves Config from OutboxInmemoryConfigToken and creates
// the Store used by an outbox feature module.
func NewStoreService(injections gioc.Injections) (*StoreService, error) {
	injections.Require(StoreServiceInjections...)
	logger := gioc.MustResolve[react.ILogger](react.LoggerToken, injections)
	config, err := resolveStoreServiceConfig(injections)
	if err != nil {
		logger.Error("initialize in-memory outbox StoreService failed: %v", err)
		return nil, err
	}
	return newStoreService(config, logger)
}

func newStoreService(config Config, logger react.ILogger) (*StoreService, error) {
	store, err := NewStore(config)
	if err != nil {
		logger.Error("initialize in-memory outbox store failed: %v", err)
		return nil, err
	}
	return &StoreService{Store: store, Logger: logger}, nil
}

// Close closes the adapter facade and logs an unexpected failure.
func (service *StoreService) Close() error {
	err := service.Store.Close()
	if err != nil {
		service.Logger.Error("close in-memory outbox store failed: %v", err)
	}
	return err
}

// InitializeOutboxStoreService implements outbox.IStoreServiceInitializer for
// the root outbox.Inmemory shorthand feature.
func (config Config) InitializeOutboxStoreService(injections gioc.Injections) (outbox.IStore, error) {
	logger := gioc.MustResolve[react.ILogger](react.LoggerToken, injections)
	return newStoreService(config, logger)
}

func resolveStoreServiceConfig(injections gioc.Injections) (Config, error) {
	value := injections.MustResolve(OutboxInmemoryConfigToken)
	switch config := value.(type) {
	case Config:
		return config, nil
	case *Config:
		if config == nil {
			return Config{}, fmt.Errorf("%w: inmemory outbox config is required", outbox.ErrInvalidArgument)
		}
		return *config, nil
	default:
		return Config{}, fmt.Errorf("%w: %s must contain inmemory.Config or *inmemory.Config", outbox.ErrInvalidArgument, OutboxInmemoryConfigToken)
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

// ProvideConfig returns a provider for OutboxInmemoryConfigToken. The copied
// configuration is exposed as a pointer to match React's configuration
// convention.
func ProvideConfig(config Config) gioc.IProvider {
	copy := config
	return gioc.ValueProvider(OutboxInmemoryConfigToken, &copy, true)
}
