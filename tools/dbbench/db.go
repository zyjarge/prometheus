package main

import (
	"fmt"
	"os"
	"path"

	"github.com/boltdb/bolt"
	"github.com/jmhodges/levigo"
	"github.com/syndtr/goleveldb/leveldb"
	leveldb_cache "github.com/syndtr/goleveldb/leveldb/cache"
	leveldb_it "github.com/syndtr/goleveldb/leveldb/iterator"
	leveldb_opt "github.com/syndtr/goleveldb/leveldb/opt"
	leveldb_util "github.com/syndtr/goleveldb/leveldb/util"
)

type TestDB interface {
	CreateKey(key string, value []byte, appendValue bool)
	Seek(key string, read bool)
	Close()
	Prune()
}

// FILES DB
type FilesDB struct {
	basePath string
	buf      []byte
}

func NewFilesDB(basePath string) *FilesDB {
	err := os.MkdirAll(basePath, 0700)
	if err != nil {
		panic(err)
	}
	return &FilesDB{
		basePath: basePath,
		buf:      make([]byte, 4096),
	}
}

func (f *FilesDB) keyToFile(key string) string {
	//	return fmt.Sprintf("%s/%c%c/%c%c/%s", f.basePath, key[0], key[1], key[2], key[3], key[4:])
	return fmt.Sprintf("%s/%c%c/%s", f.basePath, key[0], key[1], key[2:])
}

// exists returns whether the given file or directory exists or not
func exists(path string) bool {
	_, err := os.Stat(path)
	if err == nil {
		return true
	}
	if os.IsNotExist(err) {
		return false
	}
	panic(err)
}

func (f *FilesDB) CreateKey(key string, value []byte, appendValue bool) {
	filename := f.keyToFile(key)
	if !exists(path.Dir(filename)) {
		err := os.MkdirAll(path.Dir(filename), 0700)
		if err != nil {
			panic(err)
		}
	}
	var file *os.File
	var err error
	if appendValue {
		file, err = os.OpenFile(filename, os.O_RDWR|os.O_APPEND, 0660)
	} else {
		file, err = os.Create(filename)
	}
	if err != nil {
		panic(err)
	}
	defer file.Close()
	n, err := file.Write(value)
	if n != len(value) || err != nil {
		panic(err)
	}
}

func (f *FilesDB) Seek(key string, read bool) {
	file, err := os.Open(f.keyToFile(key))
	if err != nil {
		panic(err)
	}

	defer file.Close()

	for {
		n, err := file.Read(f.buf)
		if n == 0 {
			break
		}
		if err != nil {
			panic(err)
		}
	}
}

func (f *FilesDB) Close() {}
func (f *FilesDB) Prune() {}

// CGO LEVELDB
type CLevelDB struct {
	it    *levigo.Iterator
	db    *levigo.DB
	wo    *levigo.WriteOptions
	wb    *levigo.WriteBatch
	bsize int
}

func NewCLevelDB(basePath string) *CLevelDB {
	opts := levigo.NewOptions()
	opts.SetCache(levigo.NewLRUCache(50 * 1024 * 1024))
	opts.SetCreateIfMissing(true)
	if *disableCompression {
		opts.SetCompression(levigo.NoCompression)
	} else {
		opts.SetCompression(levigo.SnappyCompression)
	}
	db, err := levigo.Open(basePath, opts)
	if err != nil {
		panic(err)
	}
	if db == nil {
		panic(db)
	}

	ro := levigo.NewReadOptions()
	fmt.Println("Making snapshot...")
	snapshot := db.NewSnapshot()
	fmt.Println("Done.")
	ro.SetSnapshot(snapshot)

	return &CLevelDB{
		it: db.NewIterator(ro),
		db: db,
		wo: levigo.NewWriteOptions(),
		wb: levigo.NewWriteBatch(),
	}
}

func (l *CLevelDB) Flush() {
	if err := l.db.Write(l.wo, l.wb); err != nil {
		panic(err)
	}
	l.wb.Close()
	l.wb = levigo.NewWriteBatch()
	l.bsize = 0
}

func (l *CLevelDB) CreateKey(key string, value []byte, appendValue bool) {
	if appendValue {
		panic("appending to values not supported for CLevelDB")
	}
	l.wb.Put([]byte(key), value)
	l.bsize++
	if l.bsize > 4096 {
		l.Flush()
	}
}

func (l *CLevelDB) Seek(key string, read bool) {
	l.it.Seek([]byte(key))
	if !l.it.Valid() {
		panic("iterator invalid")
	}
	if read {
		l.it.Value()
	}
}

func (l *CLevelDB) Prune() {
	// Magic values per https://code.google.com/p/leveldb/source/browse/include/leveldb/db.h#131.
	keyspace := levigo.Range{
		Start: nil,
		Limit: nil,
	}

	l.db.CompactRange(keyspace)
}

func (l *CLevelDB) Close() {
	l.Flush()
	l.it.Close()
	l.db.Close()
}

// Native LEVELDB
type GoLevelDB struct {
	it    leveldb_it.Iterator
	db    *leveldb.DB
	wo    *leveldb_opt.WriteOptions
	wb    *leveldb.Batch
	bsize int
}

func NewGoLevelDB(basePath string) *GoLevelDB {
	var comp leveldb_opt.Compression
	if *disableCompression {
		comp = leveldb_opt.NoCompression
	} else {
		comp = leveldb_opt.SnappyCompression
	}
	opts := &leveldb_opt.Options{
		BlockCache:  leveldb_cache.NewLRUCache(50 * 1024 * 1024),
		Compression: comp,
	}
	db, err := leveldb.OpenFile(basePath, opts)
	if err != nil {
		panic(err)
	}
	if db == nil {
		panic(db)
	}

	snapshot, err := db.GetSnapshot()

	return &GoLevelDB{
		it: snapshot.NewIterator(nil, &leveldb_opt.ReadOptions{}),
		db: db,
		wo: &leveldb_opt.WriteOptions{},
		wb: &leveldb.Batch{},
	}
}

func (l *GoLevelDB) Flush() {
	if err := l.db.Write(l.wb, l.wo); err != nil {
		panic(err)
	}
	l.wb.Reset()
	l.bsize = 0
}

func (l *GoLevelDB) CreateKey(key string, value []byte, appendValue bool) {
	if appendValue {
		panic("appending to values not supported for GoLevelDB")
	}
	l.wb.Put([]byte(key), value)
	l.bsize++
	if l.bsize > 4096 {
		l.Flush()
	}
}

func (l *GoLevelDB) Seek(key string, read bool) {
	if !l.it.Seek([]byte(key)) {
		panic("key does not exist")
	}
	if read {
		l.it.Value()
	}
}

func (l *GoLevelDB) Prune() {
	keyspace := leveldb_util.Range{
		Start: nil,
		Limit: nil,
	}

	if err := l.db.CompactRange(keyspace); err != nil {
		panic(err)
	}
}

func (l *GoLevelDB) Close() {
	l.Flush()
	l.it.Release()
	l.db.Close()
}

// BoltDB
var boltBucket = []byte("index")

type keyValuePair struct {
	key   []byte
	value []byte
}

type BoltDB struct {
	db   *bolt.DB
	kvps []keyValuePair
}

func NewBoltDB(basePath string) *BoltDB {
	db, err := bolt.Open(basePath+"/bolt.db", 0600, nil)
	if err != nil {
		panic(err)
	}
	db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(boltBucket)
		return err
	})
	return &BoltDB{
		db:   db,
		kvps: make([]keyValuePair, 0, 4096),
	}
}

func (b *BoltDB) CreateKey(key string, value []byte, appendValue bool) {
	if appendValue {
		panic("appending to values not supported for BoltDB")
	}

	b.kvps = append(b.kvps, keyValuePair{
		key:   []byte(key),
		value: value,
	})

	// Batch is full, write it out.
	if len(b.kvps) == cap(b.kvps) {
		b.Flush()
	}
}

func (b *BoltDB) Flush() {
	err := b.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(boltBucket)
		if bucket == nil {
			return fmt.Errorf("boltdb bucket not found")
		}

		for _, kvp := range b.kvps {
			//err := bucket.Put(kvp.key, kvp.value)
			err := bucket.Delete(kvp.key)
			if err != nil {
				return err
			}
		}
		return nil
	})

	if err != nil {
		panic(err)
	}

	b.kvps = b.kvps[:0]
}

func (b *BoltDB) Seek(key string, read bool) {
	b.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(boltBucket)
		if bucket == nil {
			return fmt.Errorf("boltdb bucket not found")
		}
		bucket.Get([]byte(key))
		return nil
	})
}

func (b *BoltDB) Prune() {
}

func (b *BoltDB) Close() {
	b.Flush()
	b.db.Close()
}
