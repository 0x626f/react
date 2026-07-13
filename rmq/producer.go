package rmq

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/0x626f/gioc"
	"github.com/0x626f/gota/workers"
	"github.com/0x626f/react"
)

const ProducerServiceToken = gioc.Token("RMQProducerService")

var ProducerServiceInjections = react.InjectFromBase(ServiceToken)

type ProducerService struct {
	react.BaseService

	rmq      *Service
	exchange *Exchange

	mu             sync.Mutex
	requestTimeout time.Duration

	channel             Channel
	channelInterruption chan Error
	reconnectRequests   chan struct{}
	reconnecting        bool
}

func NewProducerService(injections gioc.Injections) (service *ProducerService, err error) {
	injections.Require(ProducerServiceInjections...)

	service = &ProducerService{}
	service.Bootstrap(ProducerServiceToken, injections)
	service.rmq = gioc.MustResolve[*Service](ServiceToken, injections)
	service.Ctx, service.Stop = context.WithCancel(service.rmq.Ctx)
	service.reconnectRequests = make(chan struct{}, 1)

	workers.NewWorkerOnSignal(service.Ctx, func() error {
		service.reconnectChannel()
		return nil
	}, service.reconnectRequests).Run()

	if err = service.connectChannel(); err != nil {
		service.Logger.Error("Couldn't create RMQ producer channel: %s", err.Error())
		service.requestReconnect()
		err = nil
	}

	service.ApplicationService.AddHook(func() {
		service.Stop()
		_ = service.closeChannel()
	})

	return
}

func (producer *ProducerService) Bind(exchange *Exchange) {
	producer.mu.Lock()
	defer producer.mu.Unlock()

	producer.exchange = exchange
}

func (producer *ProducerService) requestContext() (context.Context, context.CancelFunc) {
	if producer.requestTimeout > 0 {
		return context.WithTimeout(producer.rmq.Ctx, producer.requestTimeout)
	}
	return context.WithCancel(producer.rmq.Ctx)
}

func (producer *ProducerService) WithTimeout(timeout time.Duration) {
	producer.mu.Lock()
	defer producer.mu.Unlock()

	producer.requestTimeout = timeout
}

func (producer *ProducerService) connectChannel() (err error) {
	if err = producer.rmq.WaitReady(); err != nil {
		return err
	}

	channel, err := producer.rmq.Channel()
	if err != nil {
		return err
	}

	channelInterruption := make(chan Error)
	channel.NotifyClose(channelInterruption)

	producer.mu.Lock()
	producer.channel = channel
	producer.channelInterruption = channelInterruption
	producer.mu.Unlock()

	workers.NewWorkerOnEvent(producer.Ctx, func(Error) error {
		if producer.isCurrentInterruption(channelInterruption) {
			producer.requestReconnect()
		}
		return nil
	}, channelInterruption).
		OnFinish(func() {
			if producer.isCurrentInterruption(channelInterruption) {
				producer.requestReconnect()
			}
		}).
		Run()

	return nil
}

func (producer *ProducerService) isCurrentInterruption(interruption <-chan Error) bool {
	producer.mu.Lock()
	defer producer.mu.Unlock()
	return producer.channelInterruption == interruption
}

func (producer *ProducerService) requestReconnect() {
	select {
	case <-producer.Ctx.Done():
		return
	default:
	}

	producer.mu.Lock()
	if producer.reconnectRequests == nil || producer.reconnecting {
		producer.mu.Unlock()
		return
	}
	producer.reconnecting = true
	producer.mu.Unlock()

	select {
	case producer.reconnectRequests <- struct{}{}:
	default:
	}
}

func (producer *ProducerService) reconnectChannel() {
	defer func() {
		producer.mu.Lock()
		producer.reconnecting = false
		producer.mu.Unlock()
	}()

	for {
		select {
		case <-producer.Ctx.Done():
			return
		default:
		}

		if retryErr := workers.DoWithRetries(producer.Ctx, func(retry int) error {
			if retry > 0 {
				time.Sleep(producer.rmq.Config.RetryDelay)
			}

			producer.Logger.Info("Retrying to create RMQ producer channel #%d", retry)
			return producer.connectChannel()
		}, producer.rmq.Config.RetryCount); retryErr != nil {
			producer.Logger.Error("Couldn't recreate RMQ producer channel: %s", retryErr.Error())
			workers.Sleep(producer.Ctx, producer.rmq.Config.RetryDelay)
			continue
		}

		return
	}
}

func (producer *ProducerService) closeChannel() error {
	producer.mu.Lock()
	defer producer.mu.Unlock()

	if producer.channel == nil {
		return nil
	}

	return producer.channel.Close()
}

func (producer *ProducerService) Produce(publication *Publication) (err error) {
	if producer.exchange == nil {
		return
	}

	producer.mu.Lock()

	ctx, cancel := producer.requestContext()

	if producer.channel == nil || producer.channel.IsClosed() {
		producer.mu.Unlock()
		cancel()
		producer.requestReconnect()
		return fmt.Errorf("rabbit producer channel is not ready")
	}

	err = producer.channel.PublishWithContext(
		ctx,
		producer.exchange.Name,
		publication.Destination,
		publication.Mandatary,
		publication.Immediate,
		publication.Message,
	)
	producer.mu.Unlock()
	cancel()
	return err
}

func (producer *ProducerService) logConsumerError(config *Consumer, err string) {
	producer.Logger.Error("Message processing by consumer %s for queue %s has been failed", config.Tag, err)
}
