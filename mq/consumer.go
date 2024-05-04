package mq

import (
	"context"
	"encoding/json"
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	"log"
)

type Consumer[T any] struct {
	channel  *amqp.Channel
	exchange string
	queue    string
}

type Handler[T any] func(ctx context.Context, v T) error

type Message[T any] struct {
	Value    T
	Delivery amqp.Delivery
}

func (m Message[T]) Ack() error {
	return m.Delivery.Ack(false)
}

func (m Message[T]) Nack() error {
	return m.Delivery.Nack(false, true)
}

func (c Consumer[T]) Channel() *amqp.Channel {
	return c.channel
}

func (c Consumer[T]) consumeLoop(ctx context.Context, msgs <-chan amqp.Delivery, messages chan<- Message[T]) {
	for {
		select {
		case d, ok := <-msgs:
			if !ok {
				log.Printf("consume channel closed, exchange: %s, queue: %s", c.exchange, c.queue)
				close(messages)
				return
			}

			var value T
			if err := json.Unmarshal(d.Body, &value); err != nil {
				log.Printf("unmarshal: %s", err)
				continue
			}

			messages <- Message[T]{
				Value:    value,
				Delivery: d,
			}

		case <-ctx.Done():
			if err := c.channel.Close(); err != nil {
				log.Printf("close channel: %s", err)
				close(messages)
				return
			}
		}
	}
}

func (c Consumer[T]) Consume(ctx context.Context) (<-chan Message[T], error) {
	msgs, err := c.channel.Consume(
		c.queue,
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return nil, fmt.Errorf("consume: %w", err)
	}

	ch := make(chan Message[T])

	go c.consumeLoop(ctx, msgs, ch)

	return ch, nil
}

func (c Consumer[T]) Run(ctx context.Context, handler Handler[T]) error {
	return c.RunMessage(ctx, AutoAck(handler))
}

func (c Consumer[T]) RunMessage(ctx context.Context, handler Handler[Message[T]]) error {
	ch, err := c.Consume(ctx)
	if err != nil {
		return fmt.Errorf("consume: %w", err)
	}

	for msg := range ch {
		if err = handler(ctx, msg); err != nil {
			fmt.Println("handler error", c.exchange, c.queue, err)
		}
	}

	return nil
}

func AutoAck[T any](handler Handler[T]) Handler[Message[T]] {
	return func(ctx context.Context, msg Message[T]) error {
		if err := handler(ctx, msg.Value); err != nil {
			if nackErr := msg.Nack(); nackErr != nil {
				return fmt.Errorf("nack: %w caused by: %w", nackErr, err)
			}
			return err
		}

		if err := msg.Ack(); err != nil {
			return fmt.Errorf("ack: %w", err)
		}

		return nil
	}
}

func (c Consumer[T]) GetQueueSize() (int, error) {
	q, err := c.channel.QueueDeclarePassive(
		c.queue,
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return 0, fmt.Errorf("get queue: %w", err)
	}
	return q.Messages, nil
}
