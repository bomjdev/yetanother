package mq

import (
	"context"
	"encoding/json"
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	"log"
)

type (
	Handler[T any]             func(ctx context.Context, v T) error
	DeliveryHandler            Handler[amqp.Delivery]
	HandlerWithDelivery[T any] func(ctx context.Context, v T, delivery amqp.Delivery) error
	Consumer                   struct {
		exchange string
		queue    amqp.Queue
		handlers map[string]DeliveryHandler
		channel  *amqp.Channel
	}
)

func NewConsumer(options Options, queue string) (Consumer, error) {
	channel, err := options.createChannel()
	if err != nil {
		return Consumer{}, fmt.Errorf("create channel: %w", err)
	}

	if err = options.declareExchange(channel); err != nil {
		return Consumer{}, err
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
		return Consumer{}, fmt.Errorf("queue declare %q: %w", queue, err)
	}

	return Consumer{
		channel:  channel,
		exchange: options.Exchange,
		queue:    q,
		handlers: make(map[string]DeliveryHandler),
	}, nil
}

func (c Consumer) RegisterHandler(routingKey string, handler DeliveryHandler) {
	c.handlers[routingKey] = handler
}

func (c Consumer) RegisterHandlers(handlers map[string]DeliveryHandler) {
	for routingKey, handler := range handlers {
		c.RegisterHandler(routingKey, handler)
	}
}

func (c Consumer) BindKeys(routingKeys ...string) error {
	for _, routingKey := range routingKeys {
		log.Println("binding", c.queue.Name, routingKey)
		if err := c.channel.QueueBind(
			c.queue.Name,
			routingKey,
			c.exchange,
			false,
			nil,
		); err != nil {
			return fmt.Errorf("queue bind %q: %w", routingKey, err)
		}
	}
	return nil
}

func WithAutoAck[T any](handler Handler[T]) HandlerWithDelivery[T] {
	return func(ctx context.Context, v T, delivery amqp.Delivery) error {
		if err := handler(ctx, v); err != nil {
			if nackErr := delivery.Nack(false, true); nackErr != nil {
				return fmt.Errorf("nack: %w caused by: %w", nackErr, err)
			}
			return err
		}

		if err := delivery.Ack(false); err != nil {
			return fmt.Errorf("ack: %w", err)
		}

		return nil
	}
}

func WithDecoder[T any](decode func(amqp.Delivery) (T, error)) func(delivery HandlerWithDelivery[T]) DeliveryHandler {
	return func(handler HandlerWithDelivery[T]) DeliveryHandler {
		return func(ctx context.Context, delivery amqp.Delivery) error {
			value, err := decode(delivery)
			if err != nil {
				return err
			}
			return handler(ctx, value, delivery)
		}
	}
}

func WithJSONDecoder[T any](handler HandlerWithDelivery[T]) DeliveryHandler {
	return WithDecoder(jsonDecoder[T])(handler)
}

func jsonDecoder[T any](delivery amqp.Delivery) (T, error) {
	var value T
	if delivery.ContentType != "application/json" {
		return value, fmt.Errorf("%q %d unexpected content type %q", delivery.RoutingKey, delivery.DeliveryTag, delivery.ContentType)
	}
	err := json.Unmarshal(delivery.Body, &value)
	return value, err
}

func (c Consumer) Consume(ctx context.Context) error {
	deliveries, err := c.channel.Consume(
		c.queue.Name,
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return err
	}

	for {
		select {
		case delivery, open := <-deliveries:
			if !open {
				return fmt.Errorf("consume channel closed, exchange: %q, queue: %q", c.exchange, c.queue.Name)
			}

			handler, ok := c.handlers[delivery.RoutingKey]
			if !ok {
				log.Printf("unknown routing key: %q", delivery.RoutingKey)
				if err = delivery.Nack(false, true); err != nil {
					return fmt.Errorf("nack %q %d: %w", delivery.RoutingKey, delivery.DeliveryTag, err)
				}
			}

			if err = handler(ctx, delivery); err != nil {
				log.Printf("handle %q %d: %s", delivery.RoutingKey, delivery.DeliveryTag, err)
			}

		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
