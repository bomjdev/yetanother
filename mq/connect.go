package mq

import (
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
)

type Credentials struct {
	User     string
	Password string
	Host     string
	Port     int
}

func Connect(creds Credentials) (*amqp.Connection, error) {
	conn, err := amqp.Dial(fmt.Sprintf(
		"amqp://%s:%s@%s:%d/",
		creds.User,
		creds.Password,
		creds.Host,
		creds.Port,
	))
	if err != nil {
		return nil, fmt.Errorf("amqp dial: %w", err)
	}
	return conn, nil
}
