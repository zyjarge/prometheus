package index

import "encoding"

// KeyValueStore persists key/value pairs.
type KeyValueStore interface {
	Put(key, value encoding.BinaryMarshaler) error
	// Get unmarshals the result into value. It returns false if no entry
	// could be found for key. If value is nil, Get behaves like Has.
	Get(key encoding.BinaryMarshaler, value encoding.BinaryUnmarshaler) (bool, error)
	Has(key encoding.BinaryMarshaler) (bool, error)
	// Delete returns an error if key does not exist.
	Delete(key encoding.BinaryMarshaler) error

	NewBatch() Batch
	Commit(b Batch) error
	ForEach(func(kv KeyValueAccessor) error) error

	Close() error
}

// Iterator models an iterator over the keys and values in a KeyValueStore.
type Iterator interface {
	Error() error
	Valid() bool

	SeekToFirst() bool
	SeekToLast() bool
	Seek(encoding.BinaryMarshaler) bool

	Next() bool
	Previous() bool

	KeyValueAccessor

	Close() error
}

// KeyValueAccessor allows access to the key and value of an entry in a
// KeyValueStore.
type KeyValueAccessor interface {
	Key(encoding.BinaryUnmarshaler) error
	Value(encoding.BinaryUnmarshaler) error
}

// Batch allows KeyValueStore mutations to be pooled and committed together.
type Batch interface {
	Put(key, value encoding.BinaryMarshaler) error
	Delete(key encoding.BinaryMarshaler) error
	Reset()
}
