package redis

import (
	"fmt"
	"time"

	"github.com/0x626f/gioc"
)

const (
	maxStreamsWorkers      = 256
	maxStreamsChannelSize  = 1 << 16
	maxStreamsBatchSize    = 1000
	maxStreamsMessageBytes = 64 << 20
)

// StreamsConfigToken is intentionally separate from ConfigToken. The
// application must provide it only when redis.ForFeature(redis.Streams)
// is selected.
var StreamsConfigToken = gioc.NewToken("StreamsConfig")

// StreamsConfig controls the single inbound worker pool and the default
// bounded channel created by every Consume call. Zero-valued fields receive
// the values returned by DefaultStreamsConfig.
type StreamsConfig struct {
	WorkerCount          int
	ChannelSize          int
	DefaultConsumerCount int
	DefaultBatchSize     int64
	BlockTimeout         time.Duration
	ReclaimInterval      time.Duration
	ReclaimAfter         time.Duration
	MaximumDeliveries    int64
	DeadLetterSuffix     string
	RetryMinimumDelay    time.Duration
	RetryMaximumDelay    time.Duration
	MaximumMessageBytes  int
	MaximumStreamLength  int64
}

// DefaultStreamsConfig returns bounded defaults suitable for the common
// one-group, short-message case. MaximumStreamLength is zero by design: stream
// retention is an application decision and is never enabled implicitly.
func DefaultStreamsConfig() StreamsConfig {
	return StreamsConfig{
		WorkerCount:          4,
		ChannelSize:          64,
		DefaultConsumerCount: 1,
		DefaultBatchSize:     16,
		BlockTimeout:         time.Second,
		ReclaimInterval:      5 * time.Second,
		ReclaimAfter:         30 * time.Second,
		MaximumDeliveries:    10,
		DeadLetterSuffix:     ":dead",
		RetryMinimumDelay:    100 * time.Millisecond,
		RetryMaximumDelay:    2 * time.Second,
		MaximumMessageBytes:  1 << 20,
	}
}

// ProvideStreamsConfig makes the application-owned configuration
// available through StreamsConfigToken.
func ProvideStreamsConfig(config *StreamsConfig) gioc.IProvider {
	return gioc.ValueProvider(StreamsConfigToken, config, true)
}

func (config StreamsConfig) normalized() (StreamsConfig, error) {
	defaults := DefaultStreamsConfig()
	if config.WorkerCount == 0 {
		config.WorkerCount = defaults.WorkerCount
	}
	if config.ChannelSize == 0 {
		config.ChannelSize = defaults.ChannelSize
	}
	if config.DefaultConsumerCount == 0 {
		config.DefaultConsumerCount = defaults.DefaultConsumerCount
	}
	if config.DefaultBatchSize == 0 {
		config.DefaultBatchSize = defaults.DefaultBatchSize
	}
	if config.BlockTimeout == 0 {
		config.BlockTimeout = defaults.BlockTimeout
	}
	if config.ReclaimInterval == 0 {
		config.ReclaimInterval = defaults.ReclaimInterval
	}
	if config.ReclaimAfter == 0 {
		config.ReclaimAfter = defaults.ReclaimAfter
	}
	if config.MaximumDeliveries == 0 {
		config.MaximumDeliveries = defaults.MaximumDeliveries
	}
	if config.DeadLetterSuffix == "" {
		config.DeadLetterSuffix = defaults.DeadLetterSuffix
	}
	if config.RetryMinimumDelay == 0 {
		config.RetryMinimumDelay = defaults.RetryMinimumDelay
	}
	if config.RetryMaximumDelay == 0 {
		config.RetryMaximumDelay = defaults.RetryMaximumDelay
	}
	if config.MaximumMessageBytes == 0 {
		config.MaximumMessageBytes = defaults.MaximumMessageBytes
	}

	switch {
	case config.WorkerCount < 1 || config.WorkerCount > maxStreamsWorkers:
		return StreamsConfig{}, fmt.Errorf("redis streams worker count must be between 1 and %d", maxStreamsWorkers)
	case config.ChannelSize < 1 || config.ChannelSize > maxStreamsChannelSize:
		return StreamsConfig{}, fmt.Errorf("redis streams channel size must be between 1 and %d", maxStreamsChannelSize)
	case config.DefaultConsumerCount < 1 || config.DefaultConsumerCount > config.WorkerCount:
		return StreamsConfig{}, fmt.Errorf("redis streams default consumer count must be between 1 and worker count")
	case config.DefaultBatchSize < 1 || config.DefaultBatchSize > maxStreamsBatchSize:
		return StreamsConfig{}, fmt.Errorf("redis streams default batch size must be between 1 and %d", maxStreamsBatchSize)
	case config.BlockTimeout < time.Millisecond:
		return StreamsConfig{}, fmt.Errorf("redis streams block timeout must be at least one millisecond")
	case config.ReclaimInterval < time.Millisecond:
		return StreamsConfig{}, fmt.Errorf("redis streams reclaim interval must be at least one millisecond")
	case config.ReclaimAfter < config.ReclaimInterval:
		return StreamsConfig{}, fmt.Errorf("redis streams reclaim after must be at least reclaim interval")
	case config.MaximumDeliveries < 1:
		return StreamsConfig{}, fmt.Errorf("redis streams maximum deliveries must be positive")
	case config.DeadLetterSuffix == "":
		return StreamsConfig{}, fmt.Errorf("redis streams dead-letter suffix is required")
	case config.RetryMinimumDelay <= 0 || config.RetryMaximumDelay < config.RetryMinimumDelay:
		return StreamsConfig{}, fmt.Errorf("redis streams retry delay range is invalid")
	case config.MaximumMessageBytes < 1 || config.MaximumMessageBytes > maxStreamsMessageBytes:
		return StreamsConfig{}, fmt.Errorf("redis streams maximum message bytes must be between 1 and %d", maxStreamsMessageBytes)
	case config.MaximumStreamLength < 0:
		return StreamsConfig{}, fmt.Errorf("redis streams maximum stream length cannot be negative")
	}
	return config, nil
}

func (config StreamsConsumerConfig) normalized(service StreamsConfig) (StreamsConsumerConfig, error) {
	if config.ConsumerCount == 0 {
		config.ConsumerCount = service.DefaultConsumerCount
	}
	if config.BatchSize == 0 {
		config.BatchSize = service.DefaultBatchSize
	}
	if config.StartFrom == "" {
		config.StartFrom = StreamsStartBeginning
	}
	switch {
	case config.ConsumerCount < 1 || config.ConsumerCount > service.WorkerCount:
		return StreamsConsumerConfig{}, fmt.Errorf("redis stream consumer count must be between 1 and worker count")
	case config.BatchSize < 1 || config.BatchSize > maxStreamsBatchSize:
		return StreamsConsumerConfig{}, fmt.Errorf("redis stream batch size must be between 1 and %d", maxStreamsBatchSize)
	case config.StartFrom != StreamsStartBeginning && config.StartFrom != StreamsStartLatest:
		return StreamsConsumerConfig{}, fmt.Errorf("redis stream start position must be %q or %q", StreamsStartBeginning, StreamsStartLatest)
	}
	return config, nil
}
