package outbox

import (
	"fmt"
	"time"
)

// InmemoryConfig supplies deterministic dependencies, portable limits, and defaults.
type InmemoryConfig struct {
	Clock              IClock
	IDGenerator        IIDGenerator
	TokenGenerator     ITokenGenerator
	Limits             Limits
	DuplicateMode      DuplicateMode
	DefaultMaxAttempts int
	MaxLeaseDuration   time.Duration
}

// DefaultInmemoryConfig returns a production-safe API configuration for ephemeral use.
func DefaultInmemoryConfig() InmemoryConfig {
	return InmemoryConfig{
		Clock: SystemClock(), IDGenerator: CryptoIDGenerator(),
		TokenGenerator: CryptoTokenGenerator(), Limits: DefaultLimits(),
		DuplicateMode: RejectDuplicate, DefaultMaxAttempts: 10,
		MaxLeaseDuration: 5 * time.Minute,
	}
}

func (config InmemoryConfig) normalized() (InmemoryConfig, error) {
	defaults := DefaultInmemoryConfig()
	if config.Clock == nil {
		config.Clock = defaults.Clock
	}
	if config.IDGenerator == nil {
		config.IDGenerator = defaults.IDGenerator
	}
	if config.TokenGenerator == nil {
		config.TokenGenerator = defaults.TokenGenerator
	}
	config.Limits = config.Limits.Normalized()
	if !config.DuplicateMode.Valid() {
		return InmemoryConfig{}, fmt.Errorf("%w: duplicate mode", ErrInvalidArgument)
	}
	if config.DefaultMaxAttempts == 0 {
		config.DefaultMaxAttempts = defaults.DefaultMaxAttempts
	}
	if config.DefaultMaxAttempts < 1 || config.DefaultMaxAttempts > config.Limits.MaxAttempts {
		return InmemoryConfig{}, fmt.Errorf("%w: default max attempts", ErrInvalidArgument)
	}
	if config.MaxLeaseDuration == 0 {
		config.MaxLeaseDuration = defaults.MaxLeaseDuration
	}
	if config.MaxLeaseDuration < time.Microsecond {
		return InmemoryConfig{}, fmt.Errorf("%w: max lease duration", ErrInvalidArgument)
	}
	return config, nil
}
