package raven

// Marshaler generic type use for consumer message
type Marshaler[T any] interface {
	*T
	Marshal() ([]byte, error)
}

// Unmarshaler generic type use for producer message
type Unmarshaler[T any] interface {
	*T
	Unmarshal([]byte) error
}

// Message ...
type Message[T any] interface {
	Marshaler[T]
	Unmarshaler[T]
}
