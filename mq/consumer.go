package mq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	"log"
)

type (
	Handler[T any]             func(ctx context.Context, v T) error
	DeliveryHandler            Handler[amqp.Delivery]
	HandlerWithDelivery[T any] func(ctx context.Context, v T, delivery amqp.Delivery) error
	Consumer                   struct {
		channel  *Channel
		queue    string
		exchange string
		handlers map[string]DeliveryHandler
	}
)

func NewConsumer(connection *Connection, exchange, kind, queue string) (Consumer, error) {
	channel, err := connection.NewChannel()
	if err != nil {
		return Consumer{}, err
	}
	if err = channel.ExchangeDeclare(
		exchange,
		kind,
		true,
		false,
		false,
		false,
		nil,
	); err != nil {
		return Consumer{}, err
	}
	if _, err = channel.QueueDeclare(
		queue,
		true,
		false,
		false,
		false,
		nil,
	); err != nil {
		return Consumer{}, err
	}
	return Consumer{
		channel:  channel,
		exchange: exchange,
		queue:    queue,
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
		log.Println("binding", c.queue, routingKey)
		if err := c.channel.QueueBind(
			c.queue,
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
	for {
		if err := c.consume(ctx); err != nil {
			log.Println("consume error:", err)
			//return err
		}
	}
}

var errChanClosed = errors.New("channel closed")

func (c Consumer) consume(ctx context.Context) error {
	deliveries, err := c.channel.ConsumeWithContext(
		ctx,
		c.queue,
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
				log.Printf("consume channel closed, queue: %q", c.queue)
				return errChanClosed
			}

			if err = c.handleDelivery(ctx, delivery); err != nil {
				return fmt.Errorf("handle delivery: %w", err)
			}

		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (c Consumer) handleDelivery(ctx context.Context, delivery amqp.Delivery) error {
	handler, ok := c.handlers[delivery.RoutingKey]
	if !ok {
		log.Printf("unknown routing key: %q", delivery.RoutingKey)
		if err := delivery.Nack(false, true); err != nil {
			return fmt.Errorf("nack %q %d: %w", delivery.RoutingKey, delivery.DeliveryTag, err)
		}
	}

	if err := handler(ctx, delivery); err != nil {
		log.Printf("handle %q %d: %s", delivery.RoutingKey, delivery.DeliveryTag, err)
	}

	return nil
}
