package redis

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/0x626f/gioc"
	"github.com/0x626f/gota/workers"
	"github.com/0x626f/react"
	goredis "github.com/redis/go-redis/v9"
)

const (
	streamsKeyField     = "key"
	streamsPayloadField = "payload"
)

var streamsAckScript = goredis.NewScript(`
local pending = redis.call('XPENDING', KEYS[1], ARGV[1], ARGV[2], ARGV[2], 1)
if #pending == 0 then return 0 end
local delivery = pending[1]
if delivery[2] ~= ARGV[3] or tonumber(delivery[4]) ~= tonumber(ARGV[4]) then
  return -1
end
return redis.call('XACK', KEYS[1], ARGV[1], ARGV[2])
`)

var streamsDeadLetterScript = goredis.NewScript(`
local pending = redis.call('XPENDING', KEYS[1], ARGV[1], ARGV[2], ARGV[2], 1)
if #pending == 0 then return 0 end
local delivery = pending[1]
if delivery[2] ~= ARGV[3] or tonumber(delivery[4]) ~= tonumber(ARGV[4]) then
  return -1
end
redis.call('XADD', KEYS[2], '*',
  'key', ARGV[5],
  'payload', ARGV[6],
  'source_stream', KEYS[1],
  'source_group', ARGV[1],
  'source_id', ARGV[2],
  'deliveries', ARGV[4])
redis.call('XACK', KEYS[1], ARGV[1], ARGV[2])
return 1
`)

type streamsWorkerAssignment struct {
	subscription *streamsSubscription
	consumer     string
}

type streamsSubscription struct {
	ctx        context.Context
	cancel     context.CancelFunc
	stopCaller func() bool
	stream     string
	group      string
	config     StreamsConsumerConfig
	messages   chan StreamsMessage
	workers    int
	remaining  atomic.Int32

	reclaimMu   sync.Mutex
	nextReclaim time.Time
}

func (subscription *streamsSubscription) beginReclaim(now time.Time, interval time.Duration) bool {
	subscription.reclaimMu.Lock()
	defer subscription.reclaimMu.Unlock()
	if now.Before(subscription.nextReclaim) {
		return false
	}
	subscription.nextReclaim = now.Add(interval)
	return true
}

// StreamsService owns one fixed workers.Pool. Each Consume call reserves
// one or more pool workers as long-lived XREADGROUP readers and receives a
// service-sized bounded channel. It never creates a worker pool per stream.
type StreamsService struct {
	ApplicationService *react.ApplicationService
	Logger             react.ILogger

	backend *Service
	config  StreamsConfig
	ctx     context.Context
	cancel  context.CancelFunc
	pool    *workers.Pool[streamsWorkerAssignment]
	id      string

	mu               sync.Mutex
	reservedWorkers  int
	nextSubscription uint64
	subscriptions    map[*streamsSubscription]struct{}
	stopping         bool
	shutdownOnce     sync.Once
}

var _ IStreamsService = (*StreamsService)(nil)

// NewStreamsService resolves every dependency from the feature factory,
// validates the application-provided configuration, and starts the one fixed
// inbound worker pool.
func NewStreamsService(injections gioc.Injections) (*StreamsService, error) {
	injections.Require(StreamsServiceInjections...)
	baseService := gioc.MustResolve[*Service](ServiceToken, injections)
	configured := gioc.MustResolve[*StreamsConfig](StreamsConfigToken, injections)
	application := gioc.MustResolve[*react.ApplicationService](react.ApplicationContextServiceToken, injections)
	logger := gioc.MustResolve[react.ILogger](react.LoggerToken, injections)
	if baseService == nil || baseService.client == nil {
		return nil, fmt.Errorf("redis streams requires base service")
	}
	if configured == nil {
		return nil, fmt.Errorf("redis streams config is required")
	}
	if application == nil {
		return nil, fmt.Errorf("redis streams application service is required")
	}
	if logger == nil {
		return nil, fmt.Errorf("redis streams logger is required")
	}
	config, err := configured.normalized()
	if err != nil {
		return nil, err
	}
	instanceID, err := newStreamsInstanceID()
	if err != nil {
		return nil, fmt.Errorf("create redis streams consumer identity: %w", err)
	}
	ctx, cancel := application.DeriveContext()
	service := &StreamsService{
		ApplicationService: application, Logger: logger, backend: baseService,
		config: config, ctx: ctx, cancel: cancel, id: instanceID,
		subscriptions: make(map[*streamsSubscription]struct{}),
	}
	// Assignment contexts carry application cancellation. Keeping the pool
	// context independent lets shutdown close and drain every queued assignment,
	// so each subscription gets exactly one final channel close.
	service.pool = workers.NewPool(context.Background(), workers.PoolParams[streamsWorkerAssignment]{
		Callback:  service.consumeAssignment,
		Workers:   config.WorkerCount,
		QueueSize: config.WorkerCount,
	})
	service.pool.OnError(func(poolErr error) {
		service.Logger.Error("redis streams worker failed: %v", poolErr)
	})
	service.pool.OnRecovery(func(recovered any) {
		service.Logger.Error("redis streams worker recovered panic: %v", recovered)
	})
	service.pool.Run()
	application.AddPreShutdownHook(service.shutdown)
	return service, nil
}

// Publish JSON-encodes value and appends it to stream. A successful return
// means Redis accepted XADD; Redis persistence and replication remain server
// policy. No retention is applied unless MaximumStreamLength is configured.
func (service *StreamsService) Publish(ctx context.Context, stream string, key string, value any) error {
	if service == nil {
		return ErrStreamsClosed
	}
	service.mu.Lock()
	stopping := service.stopping || service.ctx.Err() != nil
	service.mu.Unlock()
	if stopping {
		return ErrStreamsClosed
	}
	if err := validateStreamsText("stream", stream, 1024); err != nil {
		return err
	}
	if err := validateStreamsText("key", key, 1024); err != nil {
		return err
	}
	if ctx == nil {
		return fmt.Errorf("redis streams publish context is required")
	}
	payload, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("encode redis stream payload: %w", err)
	}
	if len(payload) > service.config.MaximumMessageBytes {
		return fmt.Errorf("redis stream payload exceeds %d bytes", service.config.MaximumMessageBytes)
	}
	args := &goredis.XAddArgs{
		Stream: stream,
		Values: map[string]any{
			streamsKeyField:     key,
			streamsPayloadField: payload,
		},
	}
	if service.config.MaximumStreamLength > 0 {
		args.MaxLen = service.config.MaximumStreamLength
		args.Approx = true
	}
	if err = service.backend.client.XAdd(ctx, args).Err(); err != nil {
		return fmt.Errorf("publish redis stream %q: %w", stream, err)
	}
	return nil
}

// Consume creates the group when needed, reserves readers from the one service
// pool, and returns a bounded manual-acknowledgement channel. At most one
// optional config is accepted. Cancelling ctx releases the reserved workers
// and closes the returned channel.
func (service *StreamsService) Consume(
	ctx context.Context,
	group string,
	stream string,
	configs ...StreamsConsumerConfig,
) (<-chan StreamsMessage, error) {
	if service == nil {
		return nil, ErrStreamsClosed
	}
	if ctx == nil {
		return nil, fmt.Errorf("redis streams consume context is required")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := validateStreamsText("group", group, 256); err != nil {
		return nil, err
	}
	if err := validateStreamsText("stream", stream, 1024); err != nil {
		return nil, err
	}
	if len(configs) > 1 {
		return nil, fmt.Errorf("redis streams Consume accepts at most one consumer config")
	}
	consumerConfig := StreamsConsumerConfig{}
	if len(configs) == 1 {
		consumerConfig = configs[0]
	}
	consumerConfig, err := consumerConfig.normalized(service.config)
	if err != nil {
		return nil, err
	}
	if err = service.reserveWorkers(consumerConfig.ConsumerCount); err != nil {
		return nil, err
	}
	if err = service.backend.client.XGroupCreateMkStream(ctx, stream, group, string(consumerConfig.StartFrom)).Err(); err != nil && !isGroupExists(err) {
		service.releaseWorkerReservation(consumerConfig.ConsumerCount)
		return nil, fmt.Errorf("create redis stream group %q on %q: %w", group, stream, err)
	}
	if err = ctx.Err(); err != nil {
		service.releaseWorkerReservation(consumerConfig.ConsumerCount)
		return nil, err
	}

	subscriptionCtx, cancel := context.WithCancel(service.ctx)
	stopCaller := context.AfterFunc(ctx, cancel)
	subscription := &streamsSubscription{
		ctx: subscriptionCtx, cancel: cancel, stopCaller: stopCaller,
		stream: stream, group: group, config: consumerConfig,
		messages:    make(chan StreamsMessage, service.config.ChannelSize),
		workers:     consumerConfig.ConsumerCount,
		nextReclaim: time.Now().Add(service.config.ReclaimInterval),
	}
	subscription.remaining.Store(int32(consumerConfig.ConsumerCount))

	service.mu.Lock()
	if service.stopping || service.ctx.Err() != nil {
		service.reservedWorkers -= consumerConfig.ConsumerCount
		service.mu.Unlock()
		stopCaller()
		cancel()
		return nil, ErrStreamsClosed
	}
	service.nextSubscription++
	subscriptionNumber := service.nextSubscription
	service.subscriptions[subscription] = struct{}{}
	for workerIndex := 0; workerIndex < consumerConfig.ConsumerCount; workerIndex++ {
		service.pool.Queue() <- streamsWorkerAssignment{
			subscription: subscription,
			consumer:     fmt.Sprintf("react-%s-%d-%d", service.id, subscriptionNumber, workerIndex+1),
		}
	}
	service.mu.Unlock()
	return subscription.messages, nil
}

func (service *StreamsService) reserveWorkers(count int) error {
	service.mu.Lock()
	defer service.mu.Unlock()
	if service.stopping || service.ctx.Err() != nil {
		return ErrStreamsClosed
	}
	available := service.config.WorkerCount - service.reservedWorkers
	if count > available {
		return fmt.Errorf("%w: requested %d, available %d", ErrStreamsWorkerCapacity, count, available)
	}
	service.reservedWorkers += count
	return nil
}

func (service *StreamsService) releaseWorkerReservation(count int) {
	service.mu.Lock()
	service.reservedWorkers -= count
	service.mu.Unlock()
}

// Ack removes a delivery from the group's pending list. Ownership and delivery
// count are checked atomically so an old delivery cannot acknowledge a message
// that another consumer has already reclaimed. Ack is idempotent after success.
func (service *StreamsService) Ack(ctx context.Context, message StreamsMessage) error {
	if service == nil {
		return ErrStreamsClosed
	}
	if ctx == nil {
		return fmt.Errorf("redis streams ack context is required")
	}
	receipt := message.receipt
	if receipt.service != service || receipt.consumer == "" || receipt.attempts < 1 {
		return fmt.Errorf("redis stream message does not belong to this service")
	}
	if message.Stream != receipt.stream || message.Group != receipt.group || message.ID != receipt.id || message.Attempts != receipt.attempts {
		return fmt.Errorf("redis stream message delivery metadata was modified")
	}
	result, err := streamsAckScript.Run(
		ctx,
		service.backend.client,
		[]string{receipt.stream},
		receipt.group,
		receipt.id,
		receipt.consumer,
		receipt.attempts,
	).Int64()
	if err != nil {
		return fmt.Errorf("ack redis stream message %q: %w", message.ID, err)
	}
	if result < 0 {
		return fmt.Errorf("%w: message %q", ErrStreamsDeliveryLost, message.ID)
	}
	return nil
}

func (service *StreamsService) consumeAssignment(assignment streamsWorkerAssignment) error {
	subscription := assignment.subscription
	defer service.finishAssignment(subscription)
	retryDelay := service.config.RetryMinimumDelay
	for {
		if err := subscription.ctx.Err(); err != nil {
			return nil
		}
		if subscription.beginReclaim(time.Now(), service.config.ReclaimInterval) {
			if err := service.recoverPending(subscription, assignment.consumer); err != nil {
				if subscription.ctx.Err() != nil {
					return nil
				}
				service.Logger.Error("redis streams reclaim failed for stream %s group %s: %v", subscription.stream, subscription.group, err)
				if !waitStreamsRetry(subscription.ctx, retryDelay) {
					return nil
				}
				retryDelay = nextStreamsRetry(retryDelay, service.config.RetryMaximumDelay)
				continue
			}
		}

		streams, err := service.backend.client.XReadGroup(subscription.ctx, &goredis.XReadGroupArgs{
			Group: subscription.group, Consumer: assignment.consumer,
			Streams: []string{subscription.stream, ">"},
			Count:   subscription.config.BatchSize, Block: service.config.BlockTimeout,
		}).Result()
		if errors.Is(err, goredis.Nil) {
			retryDelay = service.config.RetryMinimumDelay
			continue
		}
		if err != nil {
			if subscription.ctx.Err() != nil {
				return nil
			}
			service.Logger.Error("redis streams read failed for stream %s group %s: %v", subscription.stream, subscription.group, err)
			if !waitStreamsRetry(subscription.ctx, retryDelay) {
				return nil
			}
			retryDelay = nextStreamsRetry(retryDelay, service.config.RetryMaximumDelay)
			continue
		}
		retryDelay = service.config.RetryMinimumDelay
		for _, result := range streams {
			for _, raw := range result.Messages {
				service.routeMessage(subscription, assignment.consumer, raw, 1)
			}
		}
	}
}

func (service *StreamsService) recoverPending(subscription *streamsSubscription, consumer string) error {
	pending, err := service.backend.client.XPendingExt(subscription.ctx, &goredis.XPendingExtArgs{
		Stream: subscription.stream, Group: subscription.group,
		Idle: service.config.ReclaimAfter, Start: "-", End: "+",
		Count: subscription.config.BatchSize,
	}).Result()
	if err != nil || len(pending) == 0 {
		return err
	}
	ids := make([]string, 0, len(pending))
	attempts := make(map[string]int64, len(pending))
	for _, delivery := range pending {
		ids = append(ids, delivery.ID)
		attempts[delivery.ID] = delivery.RetryCount + 1
	}
	claimed, err := service.backend.client.XClaim(subscription.ctx, &goredis.XClaimArgs{
		Stream: subscription.stream, Group: subscription.group, Consumer: consumer,
		MinIdle: service.config.ReclaimAfter, Messages: ids,
	}).Result()
	if err != nil {
		return err
	}
	for _, raw := range claimed {
		attempt := attempts[raw.ID]
		if attempt > service.config.MaximumDeliveries {
			if err = service.deadLetter(subscription, consumer, raw, attempt); err != nil {
				service.Logger.Error("redis streams dead-letter failed for stream %s group %s message %s: %v", subscription.stream, subscription.group, raw.ID, err)
			}
			continue
		}
		service.routeMessage(subscription, consumer, raw, attempt)
	}
	return nil
}

func (service *StreamsService) routeMessage(
	subscription *streamsSubscription,
	consumer string,
	raw goredis.XMessage,
	attempts int64,
) {
	key, err := streamsValue(raw.Values[streamsKeyField])
	if err != nil {
		service.Logger.Error("redis streams message %s on %s has invalid key: %v", raw.ID, subscription.stream, err)
		return
	}
	payload, err := streamsValue(raw.Values[streamsPayloadField])
	if err != nil {
		service.Logger.Error("redis streams message %s on %s has invalid payload: %v", raw.ID, subscription.stream, err)
		return
	}
	if len(key) > 1024 || len(payload) > service.config.MaximumMessageBytes {
		service.Logger.Error("redis streams message %s on %s exceeds configured field bounds", raw.ID, subscription.stream)
		return
	}
	message := StreamsMessage{
		ID: raw.ID, Stream: subscription.stream, Group: subscription.group,
		Key: key, Attempts: attempts, Payload: json.RawMessage(payload),
		receipt: streamsReceipt{
			consumer: consumer, service: service,
			stream: subscription.stream, group: subscription.group,
			id: raw.ID, attempts: attempts,
		},
	}
	select {
	case subscription.messages <- message:
	case <-subscription.ctx.Done():
	}
}

func (service *StreamsService) deadLetter(
	subscription *streamsSubscription,
	consumer string,
	raw goredis.XMessage,
	attempts int64,
) error {
	key, _ := streamsValue(raw.Values[streamsKeyField])
	payload, _ := streamsValue(raw.Values[streamsPayloadField])
	result, err := streamsDeadLetterScript.Run(
		subscription.ctx,
		service.backend.client,
		[]string{subscription.stream, subscription.stream + service.config.DeadLetterSuffix},
		subscription.group,
		raw.ID,
		consumer,
		attempts,
		key,
		payload,
	).Int64()
	if err != nil {
		return err
	}
	if result < 0 {
		return ErrStreamsDeliveryLost
	}
	if result > 0 {
		service.Logger.Warning("redis streams message %s on %s exceeded %d deliveries and moved to %s", raw.ID, subscription.stream, service.config.MaximumDeliveries, subscription.stream+service.config.DeadLetterSuffix)
	}
	return nil
}

func (service *StreamsService) finishAssignment(subscription *streamsSubscription) {
	if subscription.remaining.Add(-1) != 0 {
		return
	}
	if subscription.stopCaller != nil {
		subscription.stopCaller()
	}
	subscription.cancel()
	close(subscription.messages)
	service.mu.Lock()
	delete(service.subscriptions, subscription)
	service.reservedWorkers -= subscription.workers
	service.mu.Unlock()
}

func (service *StreamsService) shutdown() {
	service.shutdownOnce.Do(func() {
		service.mu.Lock()
		service.stopping = true
		service.cancel()
		service.pool.Close()
		service.mu.Unlock()
		service.pool.Wait()
	})
}

func newStreamsInstanceID() (string, error) {
	value := make([]byte, 8)
	if _, err := rand.Read(value); err != nil {
		return "", err
	}
	return hex.EncodeToString(value), nil
}

func validateStreamsText(field string, value string, maximum int) error {
	if value == "" {
		return fmt.Errorf("redis stream %s is required", field)
	}
	if len(value) > maximum {
		return fmt.Errorf("redis stream %s exceeds %d bytes", field, maximum)
	}
	return nil
}

func streamsValue(value any) (string, error) {
	switch typed := value.(type) {
	case string:
		return typed, nil
	case []byte:
		return string(typed), nil
	case nil:
		return "", fmt.Errorf("field is missing")
	default:
		return "", fmt.Errorf("field has unsupported type %T", value)
	}
}

func isGroupExists(err error) bool {
	return err != nil && strings.Contains(err.Error(), "BUSYGROUP")
}

func waitStreamsRetry(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}

func nextStreamsRetry(current time.Duration, maximum time.Duration) time.Duration {
	if current >= maximum/2 {
		return maximum
	}
	return current * 2
}
