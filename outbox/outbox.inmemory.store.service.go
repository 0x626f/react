package outbox

import (
	"fmt"

	"github.com/0x626f/gioc"
	"github.com/0x626f/react"
)

// InmemoryStoreServiceInjections lists the dependencies used by NewInmemoryStoreService.
var InmemoryStoreServiceInjections = []gioc.Token{OutboxInmemoryConfigToken, react.LoggerToken}

// InmemoryStoreService is the module-managed process-local store facade.
// Closing it never owns or closes an external resource.
type InmemoryStoreService struct {
	*InmemoryStore
	Logger react.ILogger
}

var _ IStore = (*InmemoryStoreService)(nil)

// NewInmemoryStoreService resolves InmemoryConfig from OutboxInmemoryConfigToken and creates
// the InmemoryStore used by an outbox feature module.
func NewInmemoryStoreService(injections gioc.Injections) (*InmemoryStoreService, error) {
	injections.Require(InmemoryStoreServiceInjections...)
	logger := gioc.MustResolve[react.ILogger](react.LoggerToken, injections)
	config, err := resolveInmemoryStoreServiceConfig(injections)
	if err != nil {
		logger.Error("initialize in-memory outbox InmemoryStoreService failed: %v", err)
		return nil, err
	}
	return newInmemoryStoreService(config, logger)
}

func newInmemoryStoreService(config InmemoryConfig, logger react.ILogger) (*InmemoryStoreService, error) {
	store, err := NewInmemoryStore(config)
	if err != nil {
		logger.Error("initialize in-memory outbox store failed: %v", err)
		return nil, err
	}
	return &InmemoryStoreService{InmemoryStore: store, Logger: logger}, nil
}

// Close closes the adapter facade and logs an unexpected failure.
func (service *InmemoryStoreService) Close() error {
	err := service.InmemoryStore.Close()
	if err != nil {
		service.Logger.Error("close in-memory outbox store failed: %v", err)
	}
	return err
}

func resolveInmemoryStoreServiceConfig(injections gioc.Injections) (InmemoryConfig, error) {
	value := injections.MustResolve(OutboxInmemoryConfigToken)
	switch config := value.(type) {
	case InmemoryConfig:
		return config, nil
	case *InmemoryConfig:
		if config == nil {
			return InmemoryConfig{}, fmt.Errorf("%w: inmemory outbox config is required", ErrInvalidArgument)
		}
		return *config, nil
	default:
		return InmemoryConfig{}, fmt.Errorf("%w: %s must contain outbox.InmemoryConfig or *outbox.InmemoryConfig", ErrInvalidArgument, OutboxInmemoryConfigToken)
	}
}

// InmemoryStoreServiceProvider provides a singleton InmemoryStoreService at token.
func InmemoryStoreServiceProvider(token gioc.Token) gioc.IProvider {
	return gioc.FactoryProvider(
		token,
		gioc.NewFactory(InmemoryStoreServiceInjections, gioc.Singleton, NewInmemoryStoreService),
		true,
	)
}

// ProvideInmemoryConfig returns a provider for OutboxInmemoryConfigToken. The copied
// configuration is exposed as a pointer to match React's configuration
// convention.
func ProvideInmemoryConfig(config InmemoryConfig) gioc.IProvider {
	copy := config
	return gioc.ValueProvider(OutboxInmemoryConfigToken, &copy, true)
}
