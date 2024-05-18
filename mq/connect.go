package mq

import (
	"context"
	"errors"
	"fmt"
	"github.com/bomjdev/yetanother/retry"
	amqp "github.com/rabbitmq/amqp091-go"
	"time"
)

type Credentials struct {
	User     string
	Password string
	Host     string
	Port     int
}

func Connect(creds Credentials) (*Connection, error) {
	return ConnectWithString(fmt.Sprintf(
		"amqp://%s:%s@%s:%d/",
		creds.User,
		creds.Password,
		creds.Host,
		creds.Port,
	))
}

func ConnectWithString(creds string) (*Connection, error) {
	conn, err := amqp.Dial(creds)
	if err != nil {
		return nil, fmt.Errorf("amqp dial: %w", err)
	}
	return &Connection{conn: conn, creds: creds, retry: retry.Factory(retry.Options{
		Attempts:  5,
		Delay:     time.Second,
		MaxDelay:  time.Minute,
		DelayFunc: retry.Double,
	})}, nil
}

type Connection struct {
	conn  *amqp.Connection
	creds string
	retry retry.Retry
}

func (c *Connection) reconnect() error {
	conn, err := amqp.Dial(c.creds)
	if err != nil {
		return fmt.Errorf("amqp reconnect dial: %w", err)
	}
	c.conn = conn
	return nil
}

func (c *Connection) Channel() (channel *amqp.Channel, err error) {
	err = c.retry(context.TODO(), func(_ context.Context) error {
		channel, err = c.conn.Channel()
		if errors.Is(err, amqp.ErrClosed) {
			if err = c.reconnect(); err != nil {
				return err
			}
			return amqp.ErrClosed
		}
		return err
	})
	return
}
