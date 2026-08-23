package rmq

import (
	"context"

	"github.com/rabbitmq/amqp091-go"
)

type IConnection interface {
	NotifyClose(chan Error) chan Error
	IsClosed() bool
	Channel() (*amqp091.Channel, error)
	Close() error
}

type IChannel interface {
	NotifyClose(chan Error) chan Error
	IsClosed() bool
	Close() error
	PublishWithContext(context.Context, string, string, bool, bool, OutcomeMessage) error
	QueueDeclare(string, bool, bool, bool, bool, Args) (amqp091.Queue, error)
	ExchangeDeclare(string, string, bool, bool, bool, bool, Args) error
	QueueBind(string, string, string, bool, Args) error
	ConsumeWithContext(context.Context, string, string, bool, bool, bool, bool, Args) (<-chan amqp091.Delivery, error)
	QueueDelete(string, bool, bool, bool) (int, error)
	QueueInspect(string) (amqp091.Queue, error)
	Get(string, bool) (amqp091.Delivery, bool, error)
}
