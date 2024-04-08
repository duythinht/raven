package raven

import (
	"context"
	"errors"
	"fmt"
	"github.com/twmb/franz-go/pkg/kgo"
)

var (
	ErrEncodeData = errors.New("error: encode data")
)

// Producer abstraction for a message, that satisfied Message interface
type Producer[T any, M Marshaler[T]] struct {
	Client *kgo.Client
}

func NewProducer[T any, M Marshaler[T]](opt ...kgo.Opt) (*Producer[T, M], error) {
	client, err := kgo.NewClient(
		opt...,
	)

	if err != nil {
		return nil, err
	}

	if err := client.Ping(context.Background()); err != nil {
		return nil, err
	}

	return &Producer[T, M]{
		client,
	}, nil
}

type PushOption func(record *kgo.Record) error

// Topic determine topic for a push record
func Topic(topic string) PushOption {
	return func(record *kgo.Record) error {
		record.Topic = topic
		return nil
	}
}

// Key determine key for a push record
func Key[T interface{ ~string | ~[]byte }](key T) PushOption {
	return func(record *kgo.Record) error {
		record.Key = []byte(key)
		return nil
	}
}

// Header add a header to a push record
func Header(header kgo.RecordHeader) PushOption {
	return func(record *kgo.Record) error {
		record.Headers = append(record.Headers, header)
		return nil
	}
}

// Push a record to the topic
func (p *Producer[T, M]) Push(ctx context.Context, data M, opts ...PushOption) (*kgo.Record, error) {
	raw, err := data.Marshal()
	if err != nil {
		return nil, fmt.Errorf("%w - %s", ErrEncodeData, err.Error())
	}

	done := make(chan struct{})

	defer close(done)

	record := &kgo.Record{
		Value: raw,
	}

	for _, opt := range opts {
		if err := opt(record); err != nil {
			return nil, err
		}
	}

	p.Client.Produce(ctx, record, func(r *kgo.Record, e error) {
		record, err = r, e
		done <- struct{}{}
	})

	<-done
	return record, err
}

// Close the producer
func (p *Producer[T, M]) Close() error {
	p.Client.Close()
	return nil
}
