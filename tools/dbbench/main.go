// Copyright 2013 Prometheus Team
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"fmt"
	"math/rand"
	"time"
	"flag"
)

var (
	useFiles = flag.Bool("useFiles", false, "Whether to use the files DB or LevelDB")
	doPopulate = flag.Bool("doPopulate", false, "Whether to populate the database")
	totalKeys = flag.Int("totalKeys", 100000, "The number of keys to populate")
	seekKeys = flag.Int("seekKeys", 10, "The number of keys to query for")
	keySize = flag.Int("keySize", 100, "The size of each key in bytes")
	valueSize = flag.Int("valueSize", 32 * 1024, "The size of each value in bytes")
	doSeek = flag.Bool("doSeek", false, "Whether to do the seek test")
	doRead = flag.Bool("doRead", false, "Whether to read the values while doing the seek test")
	doPrune = flag.Bool("doPrune", false, "Whether to compact the database")

	levelDBBasePath = flag.String("levelDBBasePath", "/srv/dbbench/leveldb", "The root path for the LevelDB-based storage")
	filesBasePath = flag.String("filesBasePath", "/srv/dbbench/files", "The root path for the files-based storage")
)


type randomDataMaker struct {
		src rand.Source
}

func (r *randomDataMaker) Read(p []byte) (n int, err error) {
		for i := range p {
				p[i] = byte(r.src.Int63() & 0xff)
		}
		return len(p), nil
}

func keyFromInt(i int) string {
	key := fmt.Sprintf("%0" + fmt.Sprintf("%d", *keySize) + "d", i)
	rKey := []byte(key)
	for i := 0; i < len(rKey); i++ {
		rKey[i] = key[len(key)-1 - i]
	}
	return string(rKey)
}

func populate(db TestDB) {
	src := rand.NewSource(23)
	value := make([]byte, *valueSize)
	randMaker := randomDataMaker{src: src}

	for i := 0; i < *totalKeys; i++ {
		n, err := randMaker.Read(value)
		if n != *valueSize || err != nil {
			panic("")
		}
		db.CreateKey(keyFromInt(i), value)
	}
}

func seekTest(db TestDB) {
	for i := 0; i < *seekKeys; i++ {
		key := keyFromInt(int(rand.Int63() % int64(*totalKeys)))
		db.Seek(key, *doRead)
	}
}

func main() {
	flag.Parse()
	rand.Seed(time.Now().Unix())

	var db TestDB
	if *useFiles {
		db = NewFilesDB(*filesBasePath)
	} else {
		db = NewLevelDB(*levelDBBasePath)
	}
	defer db.Close()

	if *doPopulate {
		fmt.Println("Starting DB population...")
		start := time.Now()
		populate(db)
		fmt.Println("BENCH_RESULT:", time.Since(start) / time.Duration(*totalKeys))
		fmt.Println("Total time:", time.Since(start))
	}

	if *doSeek {
		fmt.Println("Starting seek test...")
		start := time.Now()
		seekTest(db)
		fmt.Println("BENCH_RESULT:", time.Since(start) / time.Duration(*seekKeys))
		fmt.Println("Total time:", time.Since(start))
	}

	if *doPrune {
		db.Prune()
	}
}
