package raven_test

import (
	"context"
	"github.com/duythinht/raven"
	"github.com/twmb/franz-go/pkg/kgo"
	"testing"
	"time"
)

type Message struct {
	data string
}

func (m *Message) Marshal() ([]byte, error) {
	return []byte(m.data), nil
}

func (m *Message) Unmarshal(b []byte) error {
	m.data = string(b)
	return nil
}

func TestSimpleProducer(t *testing.T) {

	data := "Hello, World!"
	topic := "t-producer-topic"

	msg := Message{
		data: data,
	}

	p, err := raven.NewProducer[Message](
		kgo.DefaultProduceTopic(topic),
		kgo.AllowAutoTopicCreation(),
	)

	if err != nil {
		t.Fatal(err)
	}

	t.Logf("producer has been started")

	defer p.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)

	defer cancel()

	r, err := p.Push(ctx, &msg, raven.Key("test"))
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("record pushed: %+v", r)

	c, err := kgo.NewClient(
		kgo.ConsumerGroup("t-producer-consumer"),
		kgo.ConsumeTopics(topic),
	)

	iter := c.PollFetches(context.Background()).RecordIter()

	for !iter.Done() {
		record := iter.Next()
		m := &Message{}
		if err := m.Unmarshal(record.Value); err != nil {
			t.Fatal(err)
		}

		if m.data != data {
			t.Fatal()
		}
	}

}
