package mq

import (
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
)

type Options struct {
	Conn     *amqp.Connection
	Exchange string
	Kind     string
}

func (o *Options) declareExchange(ch *amqp.Channel) error {
	if err := ch.ExchangeDeclare(
		o.Exchange,
		o.Kind,
		true,
		false,
		false,
		false,
		nil,
	); err != nil {
		return fmt.Errorf("exchange declare %q: %w", o.Exchange, err)
	}

	return nil
}

// createChannel initializes and returns a new AMQP channel from the Options configuration.
func (o *Options) createChannel() (*amqp.Channel, error) {
	if o.Conn == nil {
		return nil, fmt.Errorf("amqp connection is nil")
	}

	channel, err := o.Conn.Channel()
	if err != nil {
		return nil, err
	}
	return channel, nil
}

func GetProducer[T any](options Options, routingKey string) (Producer[T], error) {
	channel, err := options.createChannel()
	if err != nil {
		return Producer[T]{}, fmt.Errorf("create channel: %w", err)
	}

	if err = options.declareExchange(channel); err != nil {
		return Producer[T]{}, err
	}

	return Producer[T]{
		channel:    channel,
		exchange:   options.Exchange,
		routingKey: routingKey,
	}, nil
}

func GetConsumer[T any](options Options, queue string, routingKeys ...string) (Consumer[T], error) {
	channel, err := options.createChannel()
	if err != nil {
		return Consumer[T]{}, fmt.Errorf("create channel: %w", err)
	}

	if err = options.declareExchange(channel); err != nil {
		return Consumer[T]{}, err
	}

	q, err := channel.QueueDeclare(
		queue,
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return Consumer[T]{}, fmt.Errorf("queue declare %q: %w", queue, err)
	}

	for _, routingKey := range routingKeys {
		if err = channel.QueueBind(
			q.Name,
			routingKey,
			options.Exchange,
			false,
			nil,
		); err != nil {
			return Consumer[T]{}, fmt.Errorf("queue bind %q: %w", routingKey, err)
		}
	}

	return Consumer[T]{
		channel:  channel,
		exchange: options.Exchange,
		queue:    queue,
	}, nil
}
