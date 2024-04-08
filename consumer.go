package raven

import (
	"context"
	"fmt"
	"github.com/twmb/franz-go/pkg/kgo"
)

type contextKey int

const (
	_ contextKey = iota
	ConsumerRecordKey
)

func consumerContext(ctx context.Context, record *kgo.Record) context.Context {
	return context.WithValue(ctx, ConsumerRecordKey, record)
}

func RecordFromContext(ctx context.Context) (*kgo.Record, bool) {
	record := ctx.Value(ConsumerRecordKey)

	r, ok := record.(*kgo.Record)
	if ok {
		return r, ok
	}
	return nil, false
}

type ConsumerMessageHandler[T any, M Unmarshaler[T]] func(ctx context.Context, m M) error

// Consumer Group
type Consumer[T any, M Unmarshaler[T]] struct {
	Client  *kgo.Client
	handler ConsumerMessageHandler[T, M]
}

func NewConsumer[T any, M Unmarshaler[T]](handler ConsumerMessageHandler[T, M], opt ...kgo.Opt) (*Consumer[T, M], error) {
	client, err := kgo.NewClient(
		opt...,
	)

	if err != nil {
		return nil, err
	}

	if err := client.Ping(context.Background()); err != nil {
		return nil, err
	}

	return &Consumer[T, M]{
		client,
		handler,
	}, nil
}

func (c *Consumer[T, M]) Run(ctx context.Context) error {

	for {
		fetches := c.Client.PollFetches(ctx)

		if err := fetches.Err(); err != nil {
			return err
		}

		iter := fetches.RecordIter()

		for !iter.Done() {
			record := iter.Next()

			var m M = new(T)

			if err := m.Unmarshal(record.Value); err != nil {
				return fmt.Errorf("error: consumer message decoding - %w", err)
			}

			if err := c.handler(ctx, m); err != nil {
				return fmt.Errorf("error: handler - %w", err)
			}
		}
	}
}
