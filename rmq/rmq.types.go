package rmq

import (
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/rabbitmq/amqp091-go"
)

type Args = amqp091.Table
type Error = *amqp091.Error

type ChannelProvider func() (IChannel, error)
type OutcomeMessage = amqp091.Publishing
type MessageHandler func(message IncomeMessage) error

type IncomeMessage amqp091.Delivery

type Retry struct {
	Count  int
	Reason string
}

func (message IncomeMessage) Ack(multiple bool) error {
	return ((amqp091.Delivery)(message)).Ack(multiple)
}

func (message IncomeMessage) Reject(requeue bool) error {
	return ((amqp091.Delivery)(message)).Reject(requeue)
}

func (message IncomeMessage) Nack(multiple, requeue bool) error {
	return ((amqp091.Delivery)(message)).Nack(multiple, requeue)
}

func (message IncomeMessage) RetryState() Retry {
	return readXDeath(message.Headers)
}

func (message IncomeMessage) Retry(multiple, requeue bool) (Retry, error) {
	retry := message.RetryState()
	return retry, message.Nack(multiple, requeue)
}

func readXDeath(headers Args) Retry {
	retry := Retry{Count: 0}
	deaths, ok := headers["x-death"].([]interface{})
	if !ok {
		return retry
	}

	for _, raw := range deaths {
		death, ok := raw.(Args)
		if !ok {
			continue
		}

		if reason, _ := death["reason"].(string); reason == "expired" {
			continue
		}

		if retry.Reason == "" {
			retry.Reason, _ = death["reason"].(string)
		}
		if count := readPositiveInt(death["count"]); count > 0 {
			retry.Count += count
		}
	}

	return retry
}

func readPositiveInt(value any) int {
	switch x := value.(type) {
	case int:
		return x
	case int32:
		return int(x)
	case int64:
		return int(x)
	case string:
		n, err := strconv.Atoi(x)
		if err != nil {
			return 0
		}
		return n
	default:
		return 0
	}
}

type Queue struct {
	Name       string
	Durable    bool
	AutoDelete bool
	Exclusive  bool
	NoWait     bool
	Args       Args

	Bindings []*Binding
	derived  []*Queue
}

func NewQueue(name string) *Queue {
	return &Queue{Name: name}
}

func (queue *Queue) SetName(name string) *Queue {
	queue.Name = name
	return queue
}

func (queue *Queue) SetDurable(durable bool) *Queue {
	queue.Durable = durable
	return queue
}

func (queue *Queue) SetAutoDelete(autoDelete bool) *Queue {
	queue.AutoDelete = autoDelete
	return queue
}

func (queue *Queue) SetExclusive(exclusive bool) *Queue {
	queue.Exclusive = exclusive
	return queue
}

func (queue *Queue) SetNoWait(noWait bool) *Queue {
	queue.NoWait = noWait
	return queue
}

func (queue *Queue) SetArgs(args Args) *Queue {
	queue.Args = args
	return queue
}

func (queue *Queue) SetBindings(bindings ...*Binding) *Queue {
	queue.Bindings = bindings
	return queue
}

func (queue *Queue) AddBindings(bindings ...*Binding) *Queue {
	queue.Bindings = append(queue.Bindings, bindings...)
	return queue
}

func (queue *Queue) SetMessageTTL(ttl time.Duration) *Queue {
	queue.ensureArgs()
	queue.Args["x-message-ttl"] = int32(ttl / time.Millisecond)
	return queue
}

func (queue *Queue) SetDLQ(exchange *Exchange, key string) *Queue {
	queue.ensureArgs()
	queue.Args["x-dead-letter-exchange"] = exchange.Name
	queue.Args["x-dead-letter-routing-key"] = key
	return queue
}

func (queue *Queue) DeriveDLQ(suffix ...string) *Queue {
	key := "retry"
	if len(suffix) > 0 {
		key = suffix[0]
	}

	var ttl any
	if queue.Args != nil {
		ttl = queue.Args["x-message-ttl"]
		delete(queue.Args, "x-message-ttl")
	}

	exchange := &Exchange{
		Name:       queue.Name + fmt.Sprintf(".%s.exchange", key),
		Kind:       "direct",
		Durable:    queue.Durable,
		AutoDelete: queue.AutoDelete,
		NoWait:     queue.NoWait,
	}

	dlq := &Queue{
		Name:       queue.Name + fmt.Sprintf(".%s.queue", key),
		Durable:    queue.Durable,
		AutoDelete: queue.AutoDelete,
		Exclusive:  queue.Exclusive,
		NoWait:     queue.NoWait,
		Bindings:   Bind(exchange, key),
	}

	if ttl != nil {
		dlq.SetArgs(Args{"x-message-ttl": ttl})
	}

	if len(queue.Bindings) > 0 {
		dlq.SetDLQ(queue.Bindings[0].Exchange, queue.Bindings[0].Key)
	}

	queue.SetDLQ(exchange, key)
	queue.derived = append(queue.derived, dlq)

	return queue
}

func (queue *Queue) ensureArgs() {
	if queue.Args == nil {
		queue.Args = Args{}
	}
}

type Binding struct {
	Key      string
	Exchange *Exchange
	NoWait   bool
	Args     Args
}

func (binding *Binding) SetKey(key string) *Binding {
	binding.Key = key
	return binding
}

func (binding *Binding) SetExchange(exchange *Exchange) *Binding {
	binding.Exchange = exchange
	return binding
}

func (binding *Binding) SetNoWait(noWait bool) *Binding {
	binding.NoWait = noWait
	return binding
}

func (binding *Binding) SetArgs(args Args) *Binding {
	binding.Args = args
	return binding
}

type Exchange struct {
	Name       string
	Kind       string
	Durable    bool
	AutoDelete bool
	Internal   bool
	NoWait     bool
	Args       Args
}

func NewExchange(name string) *Exchange {
	return &Exchange{Name: name}
}

func (exchange *Exchange) SetName(name string) *Exchange {
	exchange.Name = name
	return exchange
}

func (exchange *Exchange) SetKind(kind string) *Exchange {
	exchange.Kind = kind
	return exchange
}

func (exchange *Exchange) SetDurable(durable bool) *Exchange {
	exchange.Durable = durable
	return exchange
}

func (exchange *Exchange) SetAutoDelete(autoDelete bool) *Exchange {
	exchange.AutoDelete = autoDelete
	return exchange
}

func (exchange *Exchange) SetInternal(internal bool) *Exchange {
	exchange.Internal = internal
	return exchange
}

func (exchange *Exchange) SetNoWait(noWait bool) *Exchange {
	exchange.NoWait = noWait
	return exchange
}

func (exchange *Exchange) SetArgs(args Args) *Exchange {
	exchange.Args = args
	return exchange
}

type Consumer struct {
	Queue     *Queue
	Handler   MessageHandler
	Tag       string
	AutoAck   bool
	Exclusive bool
	NoLocal   bool
	NoWait    bool
	Args      Args

	mu     sync.Mutex
	active bool
}

func (consumer *Consumer) SetQueue(queue *Queue) *Consumer {
	consumer.Queue = queue
	return consumer
}

func (consumer *Consumer) SetHandler(handler MessageHandler) *Consumer {
	consumer.Handler = handler
	return consumer
}

func (consumer *Consumer) SetTag(tag string) *Consumer {
	consumer.Tag = tag
	return consumer
}

func (consumer *Consumer) SetAutoAck(autoAck bool) *Consumer {
	consumer.AutoAck = autoAck
	return consumer
}

func (consumer *Consumer) SetExclusive(exclusive bool) *Consumer {
	consumer.Exclusive = exclusive
	return consumer
}

func (consumer *Consumer) SetNoLocal(noLocal bool) *Consumer {
	consumer.NoLocal = noLocal
	return consumer
}

func (consumer *Consumer) SetNoWait(noWait bool) *Consumer {
	consumer.NoWait = noWait
	return consumer
}

func (consumer *Consumer) SetArgs(args Args) *Consumer {
	consumer.Args = args
	return consumer
}

type Publication struct {
	Destination string
	Mandatary   bool
	Immediate   bool
	Message     OutcomeMessage
}

func (publication *Publication) SetDestination(destination string) *Publication {
	publication.Destination = destination
	return publication
}

func (publication *Publication) SetMandatary(mandatary bool) *Publication {
	publication.Mandatary = mandatary
	return publication
}

func (publication *Publication) SetMandatory(mandatory bool) *Publication {
	publication.Mandatary = mandatory
	return publication
}

func (publication *Publication) SetImmediate(immediate bool) *Publication {
	publication.Immediate = immediate
	return publication
}

func (publication *Publication) SetMessage(message OutcomeMessage) *Publication {
	publication.Message = message
	return publication
}

func Bind(exchange *Exchange, keys ...string) (bindings []*Binding) {
	for _, key := range keys {
		bindings = append(bindings, &Binding{
			Key:      key,
			Exchange: exchange,
		})
	}
	return
}

func MultiBind(bindings ...[]*Binding) (result []*Binding) {
	for _, binding := range bindings {
		result = append(result, binding...)
	}
	return
}
