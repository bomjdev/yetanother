package main

import (
	"context"
	"github.com/bomjdev/yetanother/mq"
	"log"
	"time"
)

func main() {
	ctx := context.Background()
	log.SetFlags(log.Ltime | log.Llongfile)

	conn, err := mq.Connect("amqp://guest:guest@localhost:5672/")
	if err != nil {
		log.Fatal(err)
	}

	producer, err := mq.NewProducer(conn, "test", "direct")
	if err != nil {
		log.Fatal(err)
	}
	p := mq.WithJSONEncoder[int](producer).WithKey("test")

	consumer, err := mq.NewConsumer(conn, "test", "direct", "test")
	if err != nil {
		log.Fatal(err)
	}
	if err = consumer.BindKeys("test"); err != nil {
		log.Fatal(err)
	}

	go func() {
		i := 0
		for {
			if err = p(ctx, i); err != nil {
				log.Fatal(err)
			}
			i++
			time.Sleep(time.Second)
		}
	}()

	consumer.RegisterHandler("test", mq.WithJSONDecoder(mq.WithAutoAck(func(ctx context.Context, i int) error {
		log.Println("handler:", i)
		return nil
	})))

	log.Println(consumer.Consume(ctx))
}
