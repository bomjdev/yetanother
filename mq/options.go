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
