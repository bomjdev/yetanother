package mq

import (
	"context"
	"encoding/json"
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
)

type (
	ProducerFuncWithKey[T any] func(ctx context.Context, v T, routingKey string) error
	ProducerFunc[T any]        func(ctx context.Context, v T) error
	RawProducer                ProducerFuncWithKey[amqp.Publishing]
)

func WithEncoder[T any](encode func(T) ([]byte, error), contentType string) func(produce RawProducer) ProducerFuncWithKey[T] {
	return func(produce RawProducer) ProducerFuncWithKey[T] {
		return func(ctx context.Context, v T, routingKey string) error {
			body, err := encode(v)
			if err != nil {
				return err
			}
			return produce(ctx, amqp.Publishing{
				ContentType: contentType,
				Body:        body,
			}, routingKey)
		}
	}
}

func WithJSONEncoder[T any](produce RawProducer) ProducerFuncWithKey[T] {
	return WithEncoder[T](jsonEncoder[T], "application/json")(produce)
}

func jsonEncoder[T any](v T) ([]byte, error) {
	return json.Marshal(v)
}

func (p ProducerFuncWithKey[T]) WithKey(routingKey string) ProducerFunc[T] {
	return func(ctx context.Context, v T) error {
		return p(ctx, v, routingKey)
	}
}

func NewProducer(options Options) (RawProducer, error) {
	channel, err := options.createChannel()
	if err != nil {
		return nil, fmt.Errorf("create channel: %w", err)
	}

	if err = options.declareExchange(channel); err != nil {
		return nil, err
	}

	return func(ctx context.Context, publishing amqp.Publishing, routingKey string) error {
		return channel.PublishWithContext(
			ctx,
			options.Exchange,
			routingKey,
			false,
			false,
			publishing,
		)
	}, nil
}
