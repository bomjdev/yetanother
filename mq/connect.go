package mq

import (
	"context"
	"fmt"
	"github.com/bomjdev/yetanother/retry"
	amqp "github.com/rabbitmq/amqp091-go"
	"log"
	"time"
)

type Credentials struct {
	User     string
	Password string
	Host     string
	Port     int
}

func ConnectWithCredentials(creds Credentials) (*Connection, error) {
	return Connect(fmt.Sprintf(
		"amqp://%s:%s@%s:%d/",
		creds.User,
		creds.Password,
		creds.Host,
		creds.Port,
	))
}

var (
	defaultRetry = retry.New(
		retry.Delay(retry.DelayOptions{
			Delay: 200 * time.Millisecond,
			Func:  retry.DoubleDelay,
			Max:   time.Minute,
		}),
		retry.Timeout(time.Hour),
		func(next retry.Func) retry.Func {
			return func(ctx context.Context) error {
				err := next(ctx)
				if err != nil {
					log.Println("retry error:", err)
				}
				return err
			}
		},
	)
)

func Connect(creds string) (*Connection, error) {
	return NewConnection(creds, defaultRetry)
}

type Connection struct {
	creds string
	*amqp.Connection
	retry retry.Retry
}

func NewConnection(creds string, retry retry.Retry) (*Connection, error) {
	conn, err := amqp.Dial(creds)
	if err != nil {
		return nil, err
	}
	r := Connection{creds: creds, retry: retry, Connection: conn}
	go r.loop()
	return &r, nil
}

func (c *Connection) loop() {
	for {
		reason, ok := <-c.NotifyClose(make(chan *amqp.Error))
		log.Println("conn close:", reason, ok)
		if err := c.retry(context.TODO(), func(ctx context.Context) error {
			conn, err := amqp.Dial(c.creds)
			if err == nil {
				c.Connection = conn
			}
			return err
		}); err != nil {
			panic(err)
		}
	}
}

func (c *Connection) Channel() (ch *amqp.Channel, err error) {
	return ch, c.retry(context.TODO(), func(ctx context.Context) error {
		ch, err = c.Connection.Channel()
		return err
	})
}

func (c *Connection) NewChannel() (*Channel, error) {
	channel, err := c.Channel()
	if err != nil {
		return nil, err
	}
	ch := Channel{
		conn:    c,
		Channel: channel,
		retry:   c.retry,
	}
	go ch.loop()
	return &ch, nil
}
