package outbox

import (
	"fmt"
	"time"
)

// memoryTestStoreConfig configures the package-private service-test fixture.
type memoryTestStoreConfig struct {
	Limits             Limits
	DuplicateMode      DuplicateMode
	DefaultMaxAttempts int
	MaxLeaseDuration   time.Duration
}

// defaultMemoryTestStoreConfig returns valid fixture defaults.
func defaultMemoryTestStoreConfig() memoryTestStoreConfig {
	return memoryTestStoreConfig{
		Limits:        DefaultLimits(),
		DuplicateMode: RejectDuplicate, DefaultMaxAttempts: 10,
		MaxLeaseDuration: 5 * time.Minute,
	}
}

func (config memoryTestStoreConfig) normalized() (memoryTestStoreConfig, error) {
	defaults := defaultMemoryTestStoreConfig()
	config.Limits = config.Limits.Normalized()
	if !config.DuplicateMode.Valid() {
		return memoryTestStoreConfig{}, fmt.Errorf("%w: duplicate mode", ErrInvalidArgument)
	}
	if config.DefaultMaxAttempts == 0 {
		config.DefaultMaxAttempts = defaults.DefaultMaxAttempts
	}
	if config.DefaultMaxAttempts < 1 || config.DefaultMaxAttempts > config.Limits.MaxAttempts {
		return memoryTestStoreConfig{}, fmt.Errorf("%w: default max attempts", ErrInvalidArgument)
	}
	if config.MaxLeaseDuration == 0 {
		config.MaxLeaseDuration = defaults.MaxLeaseDuration
	}
	if config.MaxLeaseDuration < time.Microsecond {
		return memoryTestStoreConfig{}, fmt.Errorf("%w: max lease duration", ErrInvalidArgument)
	}
	return config, nil
}
