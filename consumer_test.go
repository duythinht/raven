package raven_test

import (
	"context"
	"errors"
	"github.com/duythinht/raven"
	"github.com/twmb/franz-go/pkg/kgo"
	"testing"
)

func TestConsumerFetchAndRun(t *testing.T) {

	topic := "t-raven-consumer"

	data := "test-consumer-basic"

	p, err := kgo.NewClient(kgo.DefaultProduceTopic(topic), kgo.AllowAutoTopicCreation())

	if err != nil {
		t.Fatal(err)
	}

	tMessage, _ := (&Message{
		data: data,
	}).Marshal()

	if err := p.ProduceSync(context.Background(), &kgo.Record{
		Value: tMessage,
	}).FirstErr(); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	c, err := raven.NewConsumer(func(ctx context.Context, m *Message) error {
		if m.data != data {
			t.Fatal(err)
		}

		cancel()
		return nil
	}, kgo.ConsumerGroup("t-consumer-g"), kgo.ConsumeTopics(topic))

	if err != nil {
		t.Fatal(err)
	}

	if err := c.Run(ctx); err != nil {
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	}
}
