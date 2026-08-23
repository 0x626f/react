package rmq

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/0x626f/gioc"
	"github.com/0x626f/gota/workers"
	"github.com/0x626f/react"
	"github.com/rabbitmq/amqp091-go"
)

const ServiceToken = "RmqService"

var ServiceInjections = react.InjectFromBase(
	ModuleConfigToken,
)

type Service struct {
	react.BaseConfigurableService[*ModuleConfig]

	connection             Connection
	connectionInterruption chan Error
	reconnectRequests      chan struct{}

	mu    sync.RWMutex
	ready chan struct{}
}

func NewRmqService(injections gioc.Injections) (service *Service, err error) {
	injections.Require(ServiceInjections...)

	service = &Service{}

	service.Bootstrap(ServiceToken, ModuleConfigToken, injections)
	service.Ctx, service.Stop = context.WithCancel(service.Ctx)
	service.reconnectRequests = make(chan struct{}, 1)

	if err = service.connect(); err != nil {
		return nil, err
	}

	workers.NewWorkerOnSignal(service.Ctx, func() error {
		service.reconnect()
		return nil
	}, service.reconnectRequests).Run()

	service.ApplicationService.AddHook(func() {
		service.Stop()
		_ = service.closeConnection()
	})

	return
}

func (service *Service) connect() (err error) {
	url := service.Config.buildConnectionUrl()

	connection, err := amqp091.Dial(url)
	if err != nil {
		return err
	}

	service.setConnection(connection)

	return nil
}

func (service *Service) setConnection(connection Connection) {
	interruption := make(chan Error)
	connection.NotifyClose(interruption)

	service.mu.Lock()
	service.connection = connection
	service.connectionInterruption = interruption
	if service.ready == nil {
		service.ready = make(chan struct{})
	}
	service.mu.Unlock()

	service.markReady()

	workers.NewWorkerOnEvent(service.Ctx, func(Error) error {
		if service.isCurrentInterruption(interruption) {
			service.requestReconnect()
		}
		return nil
	}, interruption).
		OnFinish(func() {
			if service.isCurrentInterruption(interruption) {
				service.requestReconnect()
			}
		}).
		Run()
}

func (service *Service) isCurrentInterruption(interruption <-chan Error) bool {
	service.mu.RLock()
	defer service.mu.RUnlock()
	return service.connectionInterruption == interruption
}

func (service *Service) requestReconnect() {
	select {
	case <-service.Ctx.Done():
		return
	default:
	}

	if !service.markRestarting() {
		return
	}

	select {
	case service.reconnectRequests <- struct{}{}:
	default:
	}
}

func (service *Service) reconnect() {
	select {
	case <-service.Ctx.Done():
		return
	default:
	}

	url := service.Config.buildConnectionUrl()
	for {
		attempt := 0
		err := workers.DoWithDelays(service.Ctx, func(time.Duration) error {
			service.Logger.Info("Retrying to connect #%d", attempt)
			attempt++

			if err := service.connect(); err != nil {
				return err
			}

			service.markReady()
			return nil
		}, retryDelays(service.Config.RetryCount, service.Config.RetryDelay)...)

		if err == nil {
			return
		}

		select {
		case <-service.Ctx.Done():
			return
		default:
		}

		if service.Config.RetryCount > 0 {
			service.Logger.Error("Couldn't reconnect to RMQ using %s", url)
		}
		workers.Sleep(service.Ctx, service.Config.RetryDelay)
	}
}

func retryDelays(count int, delay time.Duration) []time.Duration {
	if count <= 0 {
		return nil
	}

	delays := make([]time.Duration, count)
	for i := range delays {
		delays[i] = delay
	}
	return delays
}

func (service *Service) Channel() (channel Channel, err error) {
	return service.ChannelContext(service.Ctx)
}

// ChannelContext opens a channel after waiting for connection readiness while
// honoring the caller's deadline. Long-running infrastructure adapters should
// prefer it to Channel so an individual operation cannot wait for reconnect
// beyond its own context.
func (service *Service) ChannelContext(ctx context.Context) (channel Channel, err error) {
	if ctx == nil {
		return nil, fmt.Errorf("rabbit channel context is required")
	}
	if err = service.WaitReadyContext(ctx); err != nil {
		return nil, err
	}
	if err = ctx.Err(); err != nil {
		return nil, err
	}

	service.mu.RLock()
	connection := service.connection
	service.mu.RUnlock()

	if connection == nil || connection.IsClosed() {
		service.requestReconnect()
		return nil, fmt.Errorf("rabbit connection is not ready")
	}

	return connection.Channel()
}

func (service *Service) Restarting() bool {
	service.mu.RLock()
	ready := service.ready
	service.mu.RUnlock()

	if ready == nil {
		return false
	}

	select {
	case <-ready:
		return false
	default:
		return true
	}
}

func (service *Service) WaitReady() error {
	return service.WaitReadyContext(service.Ctx)
}

// WaitReadyContext waits for a usable connection until either the caller or
// the RMQ service lifecycle is cancelled.
func (service *Service) WaitReadyContext(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("rabbit readiness context is required")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	service.mu.RLock()
	ready := service.ready
	serviceCtx := service.Ctx
	service.mu.RUnlock()
	if serviceCtx == nil {
		serviceCtx = context.Background()
	}
	if err := serviceCtx.Err(); err != nil {
		return err
	}

	if ready == nil {
		return fmt.Errorf("rabbit connection is not initialized")
	}

	select {
	case <-ready:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-serviceCtx.Done():
		return serviceCtx.Err()
	}
}

func (service *Service) markRestarting() bool {
	service.mu.Lock()
	defer service.mu.Unlock()

	if service.ready == nil {
		service.ready = make(chan struct{})
		return true
	}

	select {
	case <-service.ready:
	default:
		return false
	}
	service.ready = make(chan struct{})
	return true
}

func (service *Service) markReady() {
	service.mu.Lock()
	defer service.mu.Unlock()
	select {
	case <-service.ready:
	default:
		close(service.ready)
	}
}

func (service *Service) closeConnection() error {
	service.mu.RLock()
	connection := service.connection
	service.mu.RUnlock()

	if connection == nil {
		return nil
	}

	return connection.Close()
}

func (service *Service) CreateQueues(queues ...*Queue) (result []*Queue, err error) {
	if len(queues) == 0 {
		return
	}

	for _, queue := range queues {
		var channel Channel

		if channel, err = service.Channel(); err != nil {
			return
		}

		if _, err = channel.QueueDeclare(
			queue.Name,
			queue.Durable,
			queue.AutoDelete,
			queue.Exclusive,
			queue.NoWait,
			queue.Args,
		); err != nil {
			_ = channel.Close()
			return
		}
		_ = channel.Close()

		if len(queue.Bindings) > 0 {
			for _, binding := range queue.Bindings {
				if err = service.CreateExchange(binding.Exchange); err != nil {
					return
				}

				if err = service.CreateBinding(queue, binding); err != nil {
					return
				}
			}
		}

		result = append(result, queue)

		if _, err = service.CreateQueues(queue.derived...); err != nil {
			return
		}
	}

	return
}

func (service *Service) CreateExchange(exchange *Exchange) (err error) {
	var channel Channel
	if channel, err = service.Channel(); err != nil {
		return
	}
	defer channel.Close()

	err = channel.ExchangeDeclare(
		exchange.Name,
		exchange.Kind,
		exchange.Durable,
		exchange.AutoDelete,
		exchange.Internal,
		exchange.NoWait,
		exchange.Args,
	)
	return
}

func (service *Service) CreateBinding(queue *Queue, binding *Binding) (err error) {
	var channel Channel
	if channel, err = service.Channel(); err != nil {
		return
	}
	defer channel.Close()

	err = channel.QueueBind(
		queue.Name,
		binding.Key,
		binding.Exchange.Name,
		binding.NoWait,
		binding.Args,
	)
	return
}
