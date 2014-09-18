package index

import (
	"encoding"

	"github.com/syndtr/goleveldb/leveldb"
	leveldb_iterator "github.com/syndtr/goleveldb/leveldb/iterator"
)

type levelDBIterator struct {
	it leveldb_iterator.Iterator
}

func (i *levelDBIterator) Error() error {
	return i.it.Error()
}

func (i *levelDBIterator) Valid() bool {
	return i.it.Valid()
}

func (i *levelDBIterator) SeekToFirst() bool {
	return i.it.First()
}

func (i *levelDBIterator) SeekToLast() bool {
	return i.it.Last()
}

func (i *levelDBIterator) Seek(k encoding.BinaryMarshaler) bool {
	key, err := k.MarshalBinary()
	if err != nil {
		panic(err)
	}
	return i.it.Seek(key)
}

func (i *levelDBIterator) Next() bool {
	return i.it.Next()
}

func (i *levelDBIterator) Previous() bool {
	return i.it.Prev()
}

func (i *levelDBIterator) Key(key encoding.BinaryUnmarshaler) error {
	return key.UnmarshalBinary(i.it.Key())
}

func (i *levelDBIterator) Value(value encoding.BinaryUnmarshaler) error {
	return value.UnmarshalBinary(i.it.Value())
}

func (*levelDBIterator) Close() error {
	return nil
}

type snapIter struct {
	levelDBIterator
	snap *leveldb.Snapshot
}

func (i *snapIter) Close() error {
	i.snap.Release()

	return nil
}
