package mq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	"log"
	"time"
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
		publish := func() error {
			return channel.PublishWithContext(
				ctx,
				options.Exchange,
				routingKey,
				false,
				false,
				publishing,
			)
		}
		return options.Conn.retry(ctx, func(ctx context.Context) error {
			err = publish()
			if errors.Is(err, amqp.ErrClosed) {
				channel, err = options.createChannel()
				if err != nil {
					log.Println("produce channel creation error:", err)
					time.Sleep(time.Second)
					return err
				}
				return publish()
			}
			return err
		})
	}, nil
}
