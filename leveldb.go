package main

import (
	"bytes"
	"code.google.com/p/goprotobuf/proto"
	"encoding/binary"
	"flag"
	"fmt"
	"github.com/jmhodges/levigo"
	"math"
	"math/rand"
	"os"
	"time"
)

var (
	tablePath       = flag.String("tablepath", "/tmp/benchmark", "LevelDB table path.")
	keyspaceDepth   = flag.Int("keyspacedepth", 1000, "Keyspace subkey depth.")
	lookupInterval  = flag.Int("lookupinterval", 10, "Keyspace subkey request interval.")
	lookupCount     = flag.Int("lookupcount", 10, "Keyspace subkey request count.")
	numParallelOps  = flag.Int("numParallelOps", 10, "Number of operations to run in parallel.")
	populateTable   = flag.Bool("populateTable", true, "Whether to populate the LevelDB table.")
	destroyTable    = flag.Bool("destroyTable", true, "Whether to finally destroy the LevelDB table.")
	doInitialScan   = flag.Bool("doInitialScan", false, "Whether to do a full initial scan of the LevelDB table.")
	minimumPerChunk = flag.Int("plural.minimumperchunk", 1, "The minimum number of items per chunk in plural case.")
	maximumPerChunk = flag.Int("plural.maximumperchunk", 100, "The maximum number of items per chunk in plural case.")
	expensiveCheck  = flag.Bool("bench.expensivecheck", false, "Whether to perform expensive validations.")

	intervalScanCounts = flag.Int("scancycles", 50, "The number of times the entire table is scanned.")
	keyspaceRoots      = []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q", "r", "s", "t", "u", "v", "w", "x", "y", "z"}
)

func prepareSingularLevelDb() *levigo.DB {
	options := levigo.NewOptions()
	options.SetCreateIfMissing(true)

	cache := levigo.NewLRUCache(200 * 1024 * 1024)
	options.SetCache(cache)

	database, err := levigo.Open(fmt.Sprintf("%s-singular", *tablePath), options)
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
			timestampBytes := make([]byte, 8)
			binary.BigEndian.PutUint64(timestampBytes, uint64(i))
			key := &KeySingular{
				Fingerprint: proto.String(prefix),
				Timestamp:   timestampBytes,
			}
			value := &ValueSingular{
				Value: proto.Float32(float32(i)),
			}

			keyBytes, err := proto.Marshal(key)
			if err != nil {
				panic(err)
			}
			valueBytes, err := proto.Marshal(value)
			if err != nil {
				panic(err)
			}

			err = database.Put(writeOptions, keyBytes, valueBytes)
			if err != nil {
				panic(err)
			}
		}
	}

	return database
}

func preparePluralLevelDb() *levigo.DB {
	rand.Seed(0)

	options := levigo.NewOptions()
	options.SetCreateIfMissing(true)

	cache := levigo.NewLRUCache(200 * 1024 * 1024)
	options.SetCache(cache)

	database, err := levigo.Open(fmt.Sprintf("%s-plural", *tablePath), options)
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
		remainingKeys := *keyspaceDepth

		for remainingKeys > 0 {
			proposedElements := *minimumPerChunk + rand.Intn(*maximumPerChunk-*minimumPerChunk)
			elements := int(math.Min(float64(proposedElements), float64(remainingKeys)))

			start := *keyspaceDepth - remainingKeys
			end := start + elements
			remainingKeys -= elements

			timestampBytes := make([]byte, 8)
			binary.BigEndian.PutUint64(timestampBytes, uint64(start))
			key := &KeyPlural{
				Fingerprint:   proto.String(prefix),
				OpenTimestamp: timestampBytes,
				LastTimestamp: proto.Int64(int64(end)),
			}
			value := &ValuePlural{}

			for i := start; i < end; i++ {
				value.Value = append(value.Value, &ValuePlural_Value{
					Timestamp: proto.Int64(int64(i)),
					Value:     proto.Float32(float32(i)),
				})
			}
			keyBytes, err := proto.Marshal(key)
			if err != nil {
				panic(err)
			}
			valueBytes, err := proto.Marshal(value)
			if err != nil {
				panic(err)
			}

			err = database.Put(writeOptions, keyBytes, valueBytes)
			if err != nil {
				panic(err)
			}
		}
	}

	return database
}

func testLevelDbSeekAtIntervalSingle(db *levigo.DB) {
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
				timestampBytes := make([]byte, 8)
				binary.BigEndian.PutUint64(timestampBytes, uint64(h))
				key := &KeySingular{
					Fingerprint: proto.String(keyspaceRoots[index]),
					Timestamp:   timestampBytes,
				}
				keyBytes, err := proto.Marshal(key)
				if err != nil {
					panic(err)
				}
				iterator.Seek(keyBytes)

				if !iterator.Valid() {
					panic(fmt.Sprintf("key (%s) -> invalid", keyBytes))
				}

				if *expensiveCheck {
					if bytes.Compare(iterator.Key(), keyBytes) != 0 {
						panic(fmt.Sprintf("key (%s) != retrieved (%s)", iterator.Key(), keyBytes))
					}
				}
			}
			<-workerSemaphore
			iterator.Close()
		}(index)
	}
	for w := 0; w < *numParallelOps; w++ {
		workerSemaphore <- true
	}
	fmt.Printf("Singular Iterator: %s\n", time.Since(startTime))
}

func testLevelDbSeekAtIntervalPlural(db *levigo.DB) {
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
				timestampBytes := make([]byte, 8)
				binary.BigEndian.PutUint64(timestampBytes, uint64(h))
				key := &KeyPlural{
					Fingerprint:   proto.String(keyspaceRoots[index]),
					OpenTimestamp: timestampBytes,
				}
				keyBytes, err := proto.Marshal(key)
				if err != nil {
					panic(err)
				}
				iterator.Seek(keyBytes)

				if !iterator.Valid() {
					panic(fmt.Sprintf("key (%s) -> invalid", keyBytes))
				}

				retrievedKey := &KeyPlural{}
				err = proto.Unmarshal(iterator.Key(), retrievedKey)
				if err != nil {
					panic(err)
				}

				if *expensiveCheck {
					if *key.Fingerprint != *retrievedKey.Fingerprint {
						panic(fmt.Sprintf("Fingerprint %s != Fingerprint %s", *key.Fingerprint, *retrievedKey.Fingerprint))
					}
				}

				foundOpenTimestamp := int(binary.BigEndian.Uint64(retrievedKey.OpenTimestamp))
				if foundOpenTimestamp > h {
					iterator.Prev()
					if !iterator.Valid() {
						panic("Backtrack failed!")
					}

					err = proto.Unmarshal(iterator.Key(), retrievedKey)
					if err != nil {
						panic(err)
					}

					if *expensiveCheck {
						if *key.Fingerprint != *retrievedKey.Fingerprint {
							panic(fmt.Sprintf("Fingerprint %s != Fingerprint %s", *key.Fingerprint, *retrievedKey.Fingerprint))
						}
					}

					foundOpenTimestamp = int(binary.BigEndian.Uint64(retrievedKey.OpenTimestamp))
					if foundOpenTimestamp > h {
						panic(fmt.Sprintf("Did not contain timestamp %s for %s at %d", retrievedKey, key, h))
					}
				}

				retrievedValue := &ValuePlural{}
				err = proto.Unmarshal(iterator.Value(), retrievedValue)
				if err != nil {
					panic(err)
				}

				foundValue := false
				for _, value := range retrievedValue.Value {
					if int(*value.Timestamp) == h {
						foundValue = true
						break
					}
				}

				if !foundValue {
					panic("Never found candidate!")
				}
			}
			<-workerSemaphore
			iterator.Close()
		}(index)
	}
	for w := 0; w < *numParallelOps; w++ {
		workerSemaphore <- true
	}
	fmt.Printf("Plural Iterator: %s\n", time.Since(startTime))
}

func main() {
	flag.Parse()

	singularDb := prepareSingularLevelDb()
	pluralDb := preparePluralLevelDb()
	defer func() {
		singularDb.Close()
		pluralDb.Close()
		if *destroyTable {
			os.RemoveAll(fmt.Sprintf("%s-singular", *tablePath))
			os.RemoveAll(fmt.Sprintf("%s-plural", *tablePath))
		}
	}()

	testLevelDbSeekAtIntervalSingle(singularDb)
	testLevelDbSeekAtIntervalPlural(pluralDb)
}
