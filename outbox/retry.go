package outbox

import (
	"fmt"
	"math"
	"math/rand"
	"sync"
	"time"
)

// IRetryPolicy decides whether and when a failed attempt becomes due again.
type IRetryPolicy interface {
	Next(attempt int, failure error) (delay time.Duration, retry bool)
}

// IRandomSource makes retry jitter deterministic in tests. Implementations used
// by a multi-worker service must be safe for concurrent calls.
type IRandomSource interface{ Float64() float64 }

type lockedRandom struct {
	mu     sync.Mutex
	random *rand.Rand
}

func (r *lockedRandom) Float64() float64 { r.mu.Lock(); defer r.mu.Unlock(); return r.random.Float64() }

// ExponentialBackoffConfig configures bounded exponential retry delays.
type ExponentialBackoffConfig struct {
	Minimum     time.Duration
	Maximum     time.Duration
	Multiplier  float64
	Jitter      float64
	MaxAttempts int
	Random      IRandomSource
}

// ExponentialBackoff implements bounded exponential delay with optional jitter.
type ExponentialBackoff struct{ config ExponentialBackoffConfig }

// NewExponentialBackoff validates and constructs a retry policy.
func NewExponentialBackoff(config ExponentialBackoffConfig) (*ExponentialBackoff, error) {
	if config.Minimum <= 0 {
		return nil, invalid("retry.minimum", "must be positive")
	}
	if config.Maximum < config.Minimum {
		return nil, invalid("retry.maximum", "must be at least minimum")
	}
	if config.Multiplier < 1 {
		return nil, invalid("retry.multiplier", "must be at least 1")
	}
	if config.Jitter < 0 || config.Jitter > 1 {
		return nil, invalid("retry.jitter", "must be between 0 and 1")
	}
	if config.MaxAttempts <= 0 {
		return nil, invalid("retry.max_attempts", "must be positive")
	}
	if config.Random == nil {
		config.Random = &lockedRandom{random: rand.New(rand.NewSource(time.Now().UnixNano()))}
	}
	return &ExponentialBackoff{config: config}, nil
}

// Next returns the bounded jittered delay for the completed attempt number.
func (policy *ExponentialBackoff) Next(attempt int, _ error) (time.Duration, bool) {
	if policy == nil || attempt <= 0 || attempt >= policy.config.MaxAttempts {
		return 0, false
	}
	exponent := float64(attempt - 1)
	delayFloat := float64(policy.config.Minimum) * math.Pow(policy.config.Multiplier, exponent)
	if delayFloat > float64(policy.config.Maximum) {
		delayFloat = float64(policy.config.Maximum)
	}
	if policy.config.Jitter > 0 {
		factor := 1 - policy.config.Jitter + 2*policy.config.Jitter*policy.config.Random.Float64()
		delayFloat *= factor
	}
	if delayFloat > float64(policy.config.Maximum) {
		delayFloat = float64(policy.config.Maximum)
	}
	if delayFloat < float64(policy.config.Minimum) {
		delayFloat = float64(policy.config.Minimum)
	}
	if delayFloat < 0 || math.IsInf(delayFloat, 0) || math.IsNaN(delayFloat) {
		return 0, false
	}
	return time.Duration(delayFloat), true
}

// DefaultRetryPolicy returns the library's general-purpose bounded policy.
func DefaultRetryPolicy(maxAttempts int) IRetryPolicy {
	policy, err := NewExponentialBackoff(ExponentialBackoffConfig{
		Minimum: time.Second, Maximum: time.Minute, Multiplier: 2, Jitter: .2,
		MaxAttempts: maxAttempts,
	})
	if err != nil {
		panic(fmt.Sprintf("invalid default outbox retry policy: %v", err))
	}
	return policy
}
