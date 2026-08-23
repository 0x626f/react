package outbox

import (
	"github.com/0x626f/gioc"
	"github.com/0x626f/react"
)

const (
	// OutboxModuleToken is the base token used by modules returned from
	// ForFeature. The selected storage feature is appended to this value.
	OutboxModuleToken gioc.Token = "OutboxModule"

	// OutboxServiceToken resolves the lifecycle-aware routing and worker hub.
	OutboxServiceToken gioc.Token = "OutboxService"
	// OutboxConfigToken resolves the service-level worker pool configuration.
	OutboxConfigToken gioc.Token = "OutboxConfig"
	// OutboxStoreToken resolves the selected adapter's internal aggregate store.
	OutboxStoreToken gioc.Token = "OutboxStore"
	// OutboxAppenderToken exposes only producer append capability.
	OutboxAppenderToken gioc.Token = "OutboxAppender"
	// OutboxDeliveryStoreToken exposes only fenced delivery state transitions.
	OutboxDeliveryStoreToken gioc.Token = "OutboxDeliveryStore"
	// OutboxReaderToken exposes only operational reads.
	OutboxReaderToken gioc.Token = "OutboxReader"
	// OutboxMaintenanceStoreToken exposes state-aware administrative operations.
	OutboxMaintenanceStoreToken gioc.Token = "OutboxMaintenanceStore"

	// OutboxPostgresConfigToken is consumed by PostgresStoreService.
	OutboxPostgresConfigToken gioc.Token = "OutboxPostgresConfig"
	// OutboxRedisConfigToken is consumed by RedisStoreService.
	OutboxRedisConfigToken gioc.Token = "OutboxRedisConfig"
	// OutboxInmemoryConfigToken is consumed by InmemoryStoreService.
	OutboxInmemoryConfigToken gioc.Token = "OutboxInmemoryConfig"
)

// ServiceInjections are the complete dependencies resolved by NewService.
var ServiceInjections = []gioc.Token{
	OutboxStoreToken,
	OutboxConfigToken,
	react.ApplicationContextServiceToken,
	react.LoggerToken,
}

// Tokens gives every named outbox distinct dependency-injection identities.
// No package-level registry is used.
type Tokens struct {
	Service          gioc.Token
	Store            gioc.Token
	Appender         gioc.Token
	DeliveryStore    gioc.Token
	Reader           gioc.Token
	MaintenanceStore gioc.Token
}

// NewTokens creates collision-free dependency-injection identities for one
// named outbox.
func NewTokens(name string) (Tokens, error) {
	if err := ValidateID(ID(name), DefaultLimits()); err != nil {
		return Tokens{}, invalid("name", "must use the portable identifier syntax")
	}
	prefix := "Outbox:" + name + ":"
	return Tokens{
		Service:          gioc.NewToken(prefix + "Service"),
		Store:            gioc.NewToken(prefix + "Store"),
		Appender:         gioc.NewToken(prefix + "Appender"),
		DeliveryStore:    gioc.NewToken(prefix + "DeliveryStore"),
		Reader:           gioc.NewToken(prefix + "Reader"),
		MaintenanceStore: gioc.NewToken(prefix + "MaintenanceStore"),
	}, nil
}

// ModuleTokens returns the fixed service and capability tokens exposed by
// ForFeature.
func ModuleTokens() Tokens {
	return Tokens{
		Service:          OutboxServiceToken,
		Store:            OutboxStoreToken,
		Appender:         OutboxAppenderToken,
		DeliveryStore:    OutboxDeliveryStoreToken,
		Reader:           OutboxReaderToken,
		MaintenanceStore: OutboxMaintenanceStoreToken,
	}
}
