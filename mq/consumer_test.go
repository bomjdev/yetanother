package mq

import (
	"context"
	"testing"
	"time"
)

func TestNewConsumer(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	conn, err := Connect(Credentials{
		User:     "guest",
		Password: "guest",
		Host:     "localhost",
		Port:     5672,
	})
	if err != nil {
		t.Skipf("could not connect to amqp: %s", err)
	}

	opts := Options{
		Conn:     conn,
		Exchange: "test",
		Kind:     "topic",
	}

	producer, err := NewProducer(opts)
	if err != nil {
		t.Fatal(err)
	}
	intProducer := WithJSONEncoder[int](producer)
	stringProducer := WithJSONEncoder[string](producer)
	intProducerWithKey := intProducer.WithKey("test.int")
	stringProducerWithKey := stringProducer.WithKey("test.string")

	consumer, err := NewConsumer(opts, "")
	if err != nil {
		t.Fatal(err)
	}
	err = consumer.BindKeys("test.#")
	if err != nil {
		t.Fatal(err)
	}
	consumer.RegisterHandlers(map[string]DeliveryHandler{
		"test.int": WithJSONDecoder(WithAutoAck(func(ctx context.Context, v int) error {
			t.Log("int", v)
			return nil
		})),
		"test.string": WithJSONDecoder(WithAutoAck(func(ctx context.Context, v string) error {
			t.Log("string", v)
			return nil
		})),
	})

	go consumer.Consume(ctx)

	err = intProducer(ctx, 1, "test.int")
	if err != nil {
		t.Fatal(err)
	}
	err = intProducerWithKey(ctx, 2)
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(1 * time.Second)

	err = stringProducer(ctx, "test 1", "test.string")
	if err != nil {
		t.Fatal(err)
	}
	err = stringProducerWithKey(ctx, "test 2")
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(1 * time.Second)

	cancel()
	t.Log("cancelled")
	time.Sleep(1 * time.Second)
}
