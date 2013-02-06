package main

import (
	"bytes"
	"flag"
	"fmt"
	"github.com/jmhodges/levigo"
	"github.com/prometheus/prometheus/utility/test"
	"math/rand"
	"os"
	"testing"
)

var (
	tablePath      = flag.String("bench.tablepath", "/tmp/benchmark", "LevelDB table path.")
	keyspaceDepth  = flag.Int("bench.keyspacedepth", 1000, "Keyspace subkey depth.")
	lookupInterval = flag.Int("bench.lookupinterval", 10, "Keyspace subkey request interval.")
	lookupCount    = flag.Int("bench.lookupcount", 10, "Keyspace subkey request count.")

	intervalScanCounts = flag.Int("bench.scancycles", 50, "The number of times the entire table is scanned.")
	keyspaceRoots      = []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q", "r", "s", "t", "u", "v", "w", "x", "y", "z"}
)

func prepareLevelDb() *levigo.DB {
	options := levigo.NewOptions()
	options.SetCreateIfMissing(true)

	database, err := levigo.Open(*tablePath, options)
	if err != nil {
		panic(err)
	}

	writeOptions := levigo.NewWriteOptions()
	defer writeOptions.Close()
	writeOptions.SetSync(true)

	for _, prefix := range keyspaceRoots {
		for i := 0; i <= *keyspaceDepth; i++ {
			key := fmt.Sprintf("%s%04d", prefix, i)
			value := key
			err = database.Put(writeOptions, []byte(key), []byte(value))
			if err != nil {
				panic(err)
			}
		}
	}

	return database
}

func testLevelDbSeekAtIntervalsNewIterator(t test.Tester, db *levigo.DB) {
	rand.Seed(0)

	readOptions := levigo.NewReadOptions()
	defer readOptions.Close()

	scanIterator := db.NewIterator(readOptions)
	defer scanIterator.Close()

	scanIterator.SeekToFirst()
	for scanIterator = scanIterator; scanIterator.Valid(); scanIterator.Next() {
		scanIterator.Key()
		scanIterator.Value()
	}

	keyspaceRootsCount := len(keyspaceRoots)

	for i := 0; i < *intervalScanCounts; i++ {
		index := rand.Intn(keyspaceRootsCount - 1)

		for j := 0; j < *lookupCount; j++ {
			h := j * *lookupInterval
			iterator := db.NewIterator(readOptions)
			key := fmt.Sprintf("%s%04d", keyspaceRoots[index], h)
			iterator.Seek([]byte(key))

			if !iterator.Valid() {
				panic(fmt.Sprintf("key (%s) -> invalid", key))
			}

			if bytes.Compare(iterator.Key(), []byte(key)) != 0 {
				panic(fmt.Sprintf("key (%s) != retrieved (%s)", iterator.Key(), []byte(key)))
			}

			iterator.Close()
		}
	}
}

func TestLevelDbSeekAtIntervalsNewIterator(t *testing.T) {
	defer func() {
		os.RemoveAll(*tablePath)
	}()

	db := prepareLevelDb()
	defer db.Close()

	testLevelDbSeekAtIntervalsNewIterator(t, db)
}

func BenchmarkLevelDbSeekAtIntervalsNewIterator(b *testing.B) {
	defer func() {
		os.RemoveAll(*tablePath)
	}()

	db := prepareLevelDb()
	defer db.Close()

	for i := 0; i < b.N; i++ {
		testLevelDbSeekAtIntervalsNewIterator(b, db)
	}
}

// Retaining

func testLevelDbSeekAtIntervalsRetainedIterator(t test.Tester, db *levigo.DB) {
	rand.Seed(0)

	readOptions := levigo.NewReadOptions()
	defer readOptions.Close()

	scanIterator := db.NewIterator(readOptions)
	defer scanIterator.Close()

	scanIterator.SeekToFirst()
	for scanIterator = scanIterator; scanIterator.Valid(); scanIterator.Next() {
		scanIterator.Key()
		scanIterator.Value()
	}

	keyspaceRootsCount := len(keyspaceRoots)

	iterator := db.NewIterator(readOptions)
	for i := 0; i < *intervalScanCounts; i++ {
		index := rand.Intn(keyspaceRootsCount - 1)

		for j := 0; j < *lookupCount; j++ {
			h := j * *lookupInterval
			key := fmt.Sprintf("%s%04d", keyspaceRoots[index], h)
			iterator.Seek([]byte(key))

			if !iterator.Valid() {
				panic(fmt.Sprintf("key (%s) -> invalid", key))
			}

			if bytes.Compare(iterator.Key(), []byte(key)) != 0 {
				panic(fmt.Sprintf("key (%s) != retrieved (%s)", iterator.Key(), []byte(key)))
			}

		}
	}
	iterator.Close()
}

func TestLevelDbSeekAtIntervalsRetainedIterator(t *testing.T) {
	defer func() {
		os.RemoveAll(*tablePath)
	}()

	db := prepareLevelDb()
	defer db.Close()

	testLevelDbSeekAtIntervalsRetainedIterator(t, db)
}

func BenchmarkLevelDbSeekAtIntervalsRetainedIterator(b *testing.B) {
	defer func() {
		os.RemoveAll(*tablePath)
	}()

	db := prepareLevelDb()
	defer db.Close()

	for i := 0; i < b.N; i++ {
		testLevelDbSeekAtIntervalsRetainedIterator(b, db)
	}
}
