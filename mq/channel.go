package mq

import (
	"context"
	"github.com/bomjdev/yetanother/retry"
	amqp "github.com/rabbitmq/amqp091-go"
	"log"
)

type Channel struct {
	conn *Connection
	*amqp.Channel
	retry retry.Retry
}

func (c *Channel) loop() {
	for {
		reason, ok := <-c.NotifyClose(make(chan *amqp.Error))
		log.Println("conn close:", reason, ok)
		if err := c.retry(context.TODO(), func(ctx context.Context) error {
			ch, err := c.conn.Channel()
			if err == nil {
				c.Channel = ch
			}
			return err
		}); err != nil {
			panic(err)
		}
	}
}

func (c *Channel) ConsumeWithContext(ctx context.Context, queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (ch <-chan amqp.Delivery, err error) {
	return ch, c.retry(ctx, func(ctx context.Context) error {
		ch, err = c.Channel.ConsumeWithContext(ctx, queue, consumer, autoAck, exclusive, noLocal, noWait, args)
		return err
	})
}

func (c *Channel) PublishWithContext(ctx context.Context, exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error {
	return c.retry(ctx, func(ctx context.Context) error {
		return c.Channel.PublishWithContext(ctx, exchange, key, mandatory, immediate, msg)
	})
}
