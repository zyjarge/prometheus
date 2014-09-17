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

	Close() error
}

// Batch allows KeyValueStore mutations to be pooled and committed together.
type Batch interface {
	Put(key, value encoding.BinaryMarshaler) error
	Delete(key encoding.BinaryMarshaler) error
	Reset()
}
