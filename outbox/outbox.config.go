package outbox

import (
	"fmt"
	"time"
)

// MaxClaimDestinations is the portable bound for one storage claim. Service
// rotates larger routing tables through bounded claim windows.
const MaxClaimDestinations = 16

// Config controls the service-level worker pool and delivery policy. Storage
// adapters keep their own namespace and durability configuration.
type Config struct {
	WorkerCount           int
	ClaimBatchSize        int
	LeaseDuration         time.Duration
	RenewalEnabled        bool
	LeaseRenewalThreshold time.Duration
	PollMinimumInterval   time.Duration
	PollMaximumInterval   time.Duration
	DeliveryTimeout       time.Duration
	ShutdownTimeout       time.Duration
	MaximumAttempts       int
	Retry                 ExponentialBackoffConfig
	Limits                Limits

	// These dependencies are optional application delivery policies. The service
	// supplies production defaults when they are nil.
	RetryPolicy     IRetryPolicy
	ErrorClassifier IErrorClassifier
	Owner           string
}

// DestinationsConfig binds one sink to a unique set of destinations.
// Concurrency limits all deliveries through that sink registration; zero uses
// the service worker count.
type DestinationsConfig struct {
	Destinations []string
	Concurrency  int
}

// DefaultConfig returns conservative, internally valid service defaults.
func DefaultConfig() Config {
	limits := DefaultLimits()
	return Config{
		WorkerCount: 4, ClaimBatchSize: 8,
		LeaseDuration: 30 * time.Second, RenewalEnabled: true,
		LeaseRenewalThreshold: 10 * time.Second,
		PollMinimumInterval:   50 * time.Millisecond,
		PollMaximumInterval:   2 * time.Second,
		DeliveryTimeout:       15 * time.Second,
		ShutdownTimeout:       15 * time.Second,
		MaximumAttempts:       10,
		Retry: ExponentialBackoffConfig{
			Minimum: time.Second, Maximum: time.Minute, Multiplier: 2,
			Jitter: .2, MaxAttempts: 10,
		},
		Limits: limits,
	}
}

// Validate rejects unsafe or unbounded configuration relationships.
func (config Config) Validate() error {
	limits := config.Limits.withDefaults()
	if config.WorkerCount <= 0 || config.WorkerCount > limits.MaxWorkerCount {
		return invalid("worker_count", fmt.Sprintf("must be between 1 and %d", limits.MaxWorkerCount))
	}
	if config.ClaimBatchSize <= 0 || config.ClaimBatchSize > limits.MaxClaimBatchSize {
		return invalid("claim_batch_size", fmt.Sprintf("must be between 1 and %d", limits.MaxClaimBatchSize))
	}
	if config.LeaseDuration < time.Microsecond {
		return invalid("lease_duration", "must be at least one microsecond")
	}
	if config.DeliveryTimeout <= 0 {
		return invalid("delivery_timeout", "must be positive")
	}
	if !config.RenewalEnabled && config.DeliveryTimeout >= config.LeaseDuration {
		return invalid("delivery_timeout", "must be shorter than lease_duration when renewal is disabled")
	}
	if config.RenewalEnabled && (config.LeaseRenewalThreshold <= 0 || config.LeaseRenewalThreshold >= config.LeaseDuration) {
		return invalid("lease_renewal_threshold", "must be positive and shorter than lease_duration")
	}
	if config.PollMinimumInterval <= 0 || config.PollMaximumInterval < config.PollMinimumInterval {
		return invalid("poll_interval", "minimum must be positive and maximum must be at least minimum")
	}
	if config.ShutdownTimeout <= 0 {
		return invalid("shutdown_timeout", "must be positive")
	}
	if config.MaximumAttempts <= 0 || config.MaximumAttempts > limits.MaxAttempts {
		return invalid("maximum_attempts", fmt.Sprintf("must be between 1 and %d", limits.MaxAttempts))
	}
	if isNilValue(config.RetryPolicy) {
		if config.Retry.MaxAttempts > limits.MaxAttempts {
			return invalid("retry.max_attempts", fmt.Sprintf("must not exceed %d", limits.MaxAttempts))
		}
		if _, err := NewExponentialBackoff(config.Retry); err != nil {
			return err
		}
	}
	if config.Owner != "" {
		if err := ValidateLeaseOwner(config.Owner, limits); err != nil {
			return err
		}
	}
	return nil
}

func (config Config) normalized() (Config, error) {
	config.Limits = config.Limits.withDefaults()
	if err := config.Validate(); err != nil {
		return Config{}, err
	}
	if isNilValue(config.ErrorClassifier) {
		config.ErrorClassifier = DefaultErrorClassifier()
	}
	if isNilValue(config.RetryPolicy) {
		policy, err := NewExponentialBackoff(config.Retry)
		if err != nil {
			return Config{}, err
		}
		config.RetryPolicy = policy
	}
	return config, nil
}

func (config DestinationsConfig) normalized(service Config) (DestinationsConfig, error) {
	if len(config.Destinations) == 0 {
		return DestinationsConfig{}, invalid("destinations", "must contain at least one destination")
	}
	if len(config.Destinations) > service.Limits.MaxQueryValues {
		return DestinationsConfig{}, invalid("destinations", fmt.Sprintf("must contain at most %d destinations", service.Limits.MaxQueryValues))
	}
	seen := make(map[string]struct{}, len(config.Destinations))
	result := DestinationsConfig{
		Destinations: append([]string(nil), config.Destinations...),
		Concurrency:  config.Concurrency,
	}
	for _, destination := range result.Destinations {
		if err := validateRequiredText("destinations", destination, service.Limits.MaxDestinationBytes); err != nil {
			return DestinationsConfig{}, err
		}
		if _, exists := seen[destination]; exists {
			return DestinationsConfig{}, invalid("destinations", fmt.Sprintf("contains duplicate %q", destination))
		}
		seen[destination] = struct{}{}
	}
	if result.Concurrency == 0 {
		result.Concurrency = service.WorkerCount
	}
	if result.Concurrency < 1 || result.Concurrency > service.Limits.MaxDestinationWorkers {
		return DestinationsConfig{}, invalid("concurrency", fmt.Sprintf("must be between 1 and %d", service.Limits.MaxDestinationWorkers))
	}
	return result, nil
}
