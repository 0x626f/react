package rmq

import (
	"context"
	"fmt"
	"time"

	"github.com/0x626f/gioc"
	"github.com/0x626f/gota/workers"
	"github.com/0x626f/react"
	"github.com/rabbitmq/amqp091-go"
)

const ConsumerServiceToken = gioc.Token("RMQConsumerService")

var ConsumerServiceInjections = react.InjectFromBase(ServiceToken)

type ConsumerService struct {
	react.BaseService

	rmq *Service
}

func NewConsumerService(injections gioc.Injections) (service *ConsumerService, err error) {
	injections.Require(ConsumerServiceInjections...)

	service = &ConsumerService{}
	service.Bootstrap(ConsumerServiceToken, injections)
	service.rmq = gioc.MustResolve[*Service](ServiceToken, injections)
	service.Ctx, service.Stop = context.WithCancel(service.rmq.Ctx)

	return
}

func (service *ConsumerService) Consume(consumer *Consumer) (err error) {
	consumer.mu.Lock()
	defer consumer.mu.Unlock()

	if consumer.active {
		return
	}

	var channel Channel
	channel, err = service.rmq.Channel()
	if err != nil {
		return err
	}

	messages, err := channel.ConsumeWithContext(
		service.Ctx,
		consumer.Queue.Name,
		consumer.Tag,
		consumer.AutoAck,
		consumer.Exclusive,
		consumer.NoLocal,
		consumer.NoWait,
		consumer.Args,
	)

	if err != nil {
		_ = channel.Close()
		return err
	}

	workers.NewWorkerOnEvent(service.Ctx, func(message amqp091.Delivery) error {
		return consumer.Handler(IncomeMessage(message))
	}, messages).
		OnError(func(err error) {
			service.logConsumerError(consumer, err.Error())
		}).
		OnRecovery(func(subject any) {
			service.logConsumerError(consumer, fmt.Sprintf("%v", subject))
		}).
		OnFinish(func() {
			_ = channel.Close()
			service.markInactive(consumer)
			service.reconnectConsumer(consumer)
		}).
		Run()

	consumer.active = true

	return nil
}

func (service *ConsumerService) markInactive(consumer *Consumer) {
	consumer.mu.Lock()
	defer consumer.mu.Unlock()
	consumer.active = false
}

func (service *ConsumerService) reconnectConsumer(consumer *Consumer) {
	for {
		select {
		case <-service.Ctx.Done():
			return
		default:
		}

		if retryErr := workers.DoWithRetries(service.Ctx, func(retry int) error {
			if retry > 0 {
				time.Sleep(service.rmq.Config.RetryDelay)
			}

			service.Logger.Info("Retrying to consume from RMQ queue %s #%d", consumer.Queue.Name, retry)
			return service.Consume(consumer)
		}, service.rmq.Config.RetryCount); retryErr != nil {
			service.Logger.Error("Couldn't consume from queue %s: %s", consumer.Queue.Name, retryErr.Error())
			workers.Sleep(service.Ctx, service.rmq.Config.RetryDelay)
			continue
		}

		return
	}
}

func (service *ConsumerService) logConsumerError(config *Consumer, err string) {
	service.Logger.Error("Message processing by consumer %s for queue %s has been failed", config.Tag, err)
}
