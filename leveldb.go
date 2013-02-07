package main

import (
	"bytes"
	"flag"
	"fmt"
	"github.com/jmhodges/levigo"
	"math/rand"
	"os"
	"time"
)

var (
	tablePath      = flag.String("tablepath", "/tmp/benchmark", "LevelDB table path.")
	keyspaceDepth  = flag.Int("keyspacedepth", 1000, "Keyspace subkey depth.")
	lookupInterval = flag.Int("lookupinterval", 10, "Keyspace subkey request interval.")
	lookupCount    = flag.Int("lookupcount", 10, "Keyspace subkey request count.")
	numParallelOps = flag.Int("numParallelOps", 10, "Number of operations to run in parallel.")
	populateTable  = flag.Bool("populateTable", true, "Whether to populate the LevelDB table.")
	destroyTable   = flag.Bool("destroyTable", true, "Whether to finally destroy the LevelDB table.")
	doInitialScan  = flag.Bool("doInitialScan", false, "Whether to do a full initial scan of the LevelDB table.")

	intervalScanCounts = flag.Int("scancycles", 50, "The number of times the entire table is scanned.")
	keyspaceRoots      = []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q", "r", "s", "t", "u", "v", "w", "x", "y", "z"}
)

func prepareLevelDb() *levigo.DB {
	options := levigo.NewOptions()
	options.SetCreateIfMissing(true)

	cache := levigo.NewLRUCache(200 * 1024 * 1024)
	options.SetCache(cache)

	database, err := levigo.Open(*tablePath, options)
	if err != nil {
		panic(err)
	}

	writeOptions := levigo.NewWriteOptions()
	defer writeOptions.Close()
	writeOptions.SetSync(false)

	if !*populateTable {
		return database
	}
	for _, prefix := range keyspaceRoots {
		for i := 0; i <= *keyspaceDepth; i++ {
			key := fmt.Sprintf("%s%08d", prefix, i)
			value := key
			err = database.Put(writeOptions, []byte(key), []byte(value))
			if err != nil {
				panic(err)
			}
		}
	}

	return database
}

func testLevelDbSeekAtIntervalsNewIterator(db *levigo.DB) {
	rand.Seed(0)

	readOptions := levigo.NewReadOptions()
	defer readOptions.Close()

	if *doInitialScan {
		scanIterator := db.NewIterator(readOptions)
		defer scanIterator.Close()

		scanIterator.SeekToFirst()
		for scanIterator = scanIterator; scanIterator.Valid(); scanIterator.Next() {
			scanIterator.Key()
			scanIterator.Value()
		}
	}

	keyspaceRootsCount := len(keyspaceRoots)

	startTime := time.Now()
	workerSemaphore := make(chan bool, *numParallelOps)
	for i := 0; i < *intervalScanCounts; i++ {
		index := rand.Intn(keyspaceRootsCount - 1)

		workerSemaphore <- true
		go func(index int) {
			for j := 0; j < *lookupCount; j++ {
				h := j * *lookupInterval
				iterator := db.NewIterator(readOptions)
				key := fmt.Sprintf("%s%08d", keyspaceRoots[index], h)
				iterator.Seek([]byte(key))

				if !iterator.Valid() {
					panic(fmt.Sprintf("key (%s) -> invalid", key))
				}

				if bytes.Compare(iterator.Key(), []byte(key)) != 0 {
					panic(fmt.Sprintf("key (%s) != retrieved (%s)", iterator.Key(), []byte(key)))
				}

				iterator.Close()
			}
			<-workerSemaphore
		}(index)
	}
	for w := 0; w < *numParallelOps; w++ {
		workerSemaphore <- true
	}
	fmt.Printf("NewIterator: %s\n", time.Since(startTime))
}

// Retaining

func testLevelDbSeekAtIntervalsRetainedIterator(db *levigo.DB) {
	rand.Seed(0)

	readOptions := levigo.NewReadOptions()
	defer readOptions.Close()

	if *doInitialScan {
		scanIterator := db.NewIterator(readOptions)
		defer scanIterator.Close()

		scanIterator.SeekToFirst()
		for scanIterator = scanIterator; scanIterator.Valid(); scanIterator.Next() {
			scanIterator.Key()
			scanIterator.Value()
		}
	}

	keyspaceRootsCount := len(keyspaceRoots)

	startTime := time.Now()
	workerSemaphore := make(chan bool, *numParallelOps)
	for i := 0; i < *intervalScanCounts; i++ {
		iterator := db.NewIterator(readOptions)
		index := rand.Intn(keyspaceRootsCount - 1)

		workerSemaphore <- true
		go func(index int) {
			for j := 0; j < *lookupCount; j++ {
				h := j * *lookupInterval
				key := fmt.Sprintf("%s%08d", keyspaceRoots[index], h)
				iterator.Seek([]byte(key))

				if !iterator.Valid() {
					panic(fmt.Sprintf("key (%s) -> invalid", key))
				}

				if bytes.Compare(iterator.Key(), []byte(key)) != 0 {
					panic(fmt.Sprintf("key (%s) != retrieved (%s)", iterator.Key(), []byte(key)))
				}
			}
			<-workerSemaphore
			iterator.Close()
		}(index)
	}
	for w := 0; w < *numParallelOps; w++ {
		workerSemaphore <- true
	}
	fmt.Printf("RetainedIterator: %s\n", time.Since(startTime))
}

func main() {
	flag.Parse()

	db := prepareLevelDb()
	defer func() {
		db.Close()
		if *destroyTable {
			os.RemoveAll(*tablePath)
		}
	}()

	testLevelDbSeekAtIntervalsRetainedIterator(db)
	testLevelDbSeekAtIntervalsNewIterator(db)
}
