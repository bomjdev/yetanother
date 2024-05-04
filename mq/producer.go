package mq

import (
	"context"
	"encoding/json"
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
)

type Producer[T any] struct {
	channel    *amqp.Channel
	exchange   string
	routingKey string
}

func (p Producer[T]) Produce(ctx context.Context, v T) error {
	body, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}

	if err = p.channel.PublishWithContext(
		ctx,
		p.exchange,
		p.routingKey,
		false,
		false,
		amqp.Publishing{
			ContentType: "application/json",
			Body:        body,
		},
	); err != nil {
		return fmt.Errorf("publish: %w", err)
	}

	return nil
}
