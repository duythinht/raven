## Painless consumer & producer for kafka - based on Franz

### Easy to use by define a message struct, that satisfy Marshal, Unmarshaler interfaces

```go

type Message struct {
	
}

// Marshal must be impl for producer
func (*Message) Marshal() ([]byte, error) {
	panic("not yet implement")
}


// Unmarshal must be impl for consumer
func (*Message) Unmarhsal(raw []byte) error {
    panic("not yet implement")
}

```

### Producer

```go
producer, err := raven.NewProducer[Message](
    kgo.DefaultProduceTopic(topic),
    kgo.AllowAutoTopicCreation(),
)

if err != nil {
    panic(err)
}

defer p.Close()


r, err := producer.Push(context.TODO(), &msg, raven.Key("test"))
```

### Consumer

```go
consumer, err := raven.NewConsumer(func(ctx context.Context, m *Message) error {
    if m.data != data {
        t.Fatal(err)
    }

    cancel()
    return nil
}, kgo.ConsumerGroup("t-consumer-g"), kgo.ConsumeTopics(topic))

if err != nil {
    t.Fatal(err)
}

if err := consumer.Run(ctx); err != nil {
    if !errors.Is(err, context.Canceled) {
        t.Fatal(err)
    }
}
```