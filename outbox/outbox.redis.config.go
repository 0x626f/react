package outbox

import (
	"fmt"
	"time"
)

// RedisConfig selects the same-slot namespace, resource limits, and required
// durability and eviction posture.
type RedisConfig struct {
	Namespace          string
	DuplicateMode      DuplicateMode
	DefaultMaxAttempts int
	MaxLeaseDuration   time.Duration
	// MaxAppendEncodedBytes bounds the complete JSON batch evaluated by Lua.
	MaxAppendEncodedBytes int
	// MaxClaimResponseBytes bounds record data returned by one claim script.
	MaxClaimResponseBytes int
	Limits                Limits
	DurabilityMode        RedisDurabilityMode
	RequireNoEviction     bool
	// AllowUnsafeEviction must be set deliberately to permit construction when
	// maxmemory-policy is not required to be noeviction.
	AllowUnsafeEviction bool
}

// DefaultRedisConfig requires noeviction and warns when AOF is unavailable.
func DefaultRedisConfig() RedisConfig {
	return RedisConfig{
		Namespace: "default", DuplicateMode: RejectDuplicate,
		DefaultMaxAttempts: 10, MaxLeaseDuration: 5 * time.Minute,
		MaxAppendEncodedBytes: 8 << 20, MaxClaimResponseBytes: 9 << 20,
		Limits: DefaultLimits(), DurabilityMode: RedisDurabilityWarn,
		RequireNoEviction: true,
	}
}

func (config RedisConfig) normalized() (RedisConfig, error) {
	defaults := DefaultRedisConfig()
	if config.Namespace == "" {
		config.Namespace = defaults.Namespace
	}
	if _, err := NewRedisKeys(config.Namespace); err != nil {
		return RedisConfig{}, err
	}
	if !config.DuplicateMode.Valid() {
		return RedisConfig{}, fmt.Errorf("%w: duplicate mode", ErrInvalidArgument)
	}
	if config.DurabilityMode == "" {
		config.DurabilityMode = defaults.DurabilityMode
	}
	if !config.AllowUnsafeEviction {
		config.RequireNoEviction = true
	}
	if config.DurabilityMode != RedisDurabilityUnchecked && config.DurabilityMode != RedisDurabilityWarn && config.DurabilityMode != RedisDurabilityRequireAOF {
		return RedisConfig{}, fmt.Errorf("%w: durability mode", ErrInvalidArgument)
	}
	config.Limits = config.Limits.Normalized()
	if config.DefaultMaxAttempts == 0 {
		config.DefaultMaxAttempts = defaults.DefaultMaxAttempts
	}
	if config.DefaultMaxAttempts < 1 || config.DefaultMaxAttempts > config.Limits.MaxAttempts {
		return RedisConfig{}, fmt.Errorf("%w: default max attempts", ErrInvalidArgument)
	}
	if config.MaxLeaseDuration == 0 {
		config.MaxLeaseDuration = defaults.MaxLeaseDuration
	}
	if config.MaxLeaseDuration < time.Microsecond {
		return RedisConfig{}, fmt.Errorf("%w: max lease duration", ErrInvalidArgument)
	}
	if config.MaxAppendEncodedBytes == 0 {
		config.MaxAppendEncodedBytes = defaults.MaxAppendEncodedBytes
	}
	if config.MaxClaimResponseBytes == 0 {
		config.MaxClaimResponseBytes = defaults.MaxClaimResponseBytes
	}
	if config.MaxAppendEncodedBytes < 1024 || config.MaxAppendEncodedBytes > 64<<20 {
		return RedisConfig{}, fmt.Errorf("%w: max append encoded bytes", ErrInvalidArgument)
	}
	claimMinimum := config.MaxAppendEncodedBytes + config.Limits.MaxLeaseOwnerBytes + config.Limits.MaxLeaseTokenBytes + 1024
	if config.MaxClaimResponseBytes < claimMinimum || config.MaxClaimResponseBytes > 64<<20 {
		return RedisConfig{}, fmt.Errorf("%w: max claim response bytes", ErrInvalidArgument)
	}
	return config, nil
}
