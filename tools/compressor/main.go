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
	"bytes"
	"compress/flate"
	"compress/gzip"
	"compress/lzw"
	"compress/zlib"
	"encoding/binary"
	"flag"
	"io/ioutil"
	"math"
	"os"
	"os/exec"

	"code.google.com/p/snappy-go/snappy"

	"github.com/golang/glog"

	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/storage/metric"
	"github.com/prometheus/prometheus/storage/metric/tiered"
)

var (
	storageRoot   = flag.String("storage.root", "", "The path to the storage root for Prometheus.")
	dieOnBadChunk = flag.Bool("dieOnBadChunk", false, "Whether to die upon encountering a bad chunk.")
)

const (
	sampleSize       = 16
	minimumChunkSize = 5000
)

type compressFn func([]byte) int

type SamplesCompressor struct {
	dest              []byte
	chunks            int
	samples           int
	uncompressedBytes int
	compressors       map[string]compressFn
	compressedBytes   map[string]int
	pendingSamples    metric.Values

	prevValue     float64
	float32Values int
	intValues     int
	int8Deltas    int
	int16Deltas   int
	int32Deltas   int
	int64Deltas   int
}

func (c *SamplesCompressor) Operate(key, value interface{}) *storage.OperatorError {
	v := value.(metric.Values)
	c.pendingSamples = append(c.pendingSamples, v...)
	if len(c.pendingSamples) < minimumChunkSize {
		return nil
	}

	glog.Info("Chunk size: ", len(c.pendingSamples))
	c.chunks++
	c.samples += len(c.pendingSamples)

	sz := len(c.pendingSamples) * sampleSize
	if cap(c.dest) < sz {
		c.dest = make([]byte, sz)
	} else {
		c.dest = c.dest[0:sz]
	}

	c.prevValue = float64(c.pendingSamples[0].Value)
	for i, sample := range c.pendingSamples {
		val := float64(sample.Value)
		if float64(float32(val)) == val {
			c.float32Values++
		}
		if float64(int64(val)) == val {
			c.intValues++
		}
		if i > 0 {
			delta := c.prevValue - val
			if delta < 0 {
				delta = -delta
			}
			if float64(int64(delta)) == delta {
				if delta < 2^8 {
					c.int8Deltas++
				} else if delta < 2^16 {
					c.int16Deltas++
				} else if delta < 2^32 {
					c.int32Deltas++
				} else if delta < 2^64 {
					c.int64Deltas++
				}
			}

			c.prevValue = val
		}

		offset := i * sampleSize
		binary.LittleEndian.PutUint64(c.dest[offset:], uint64(sample.Timestamp.Unix()))
		binary.LittleEndian.PutUint64(c.dest[offset+8:], math.Float64bits(float64(sample.Value)))
	}
	c.uncompressedBytes += sz

	for algo, fn := range c.compressors {
		c.compressedBytes[algo] += fn(c.dest)
	}

	c.pendingSamples = nil
	return nil
}

func (c *SamplesCompressor) Report() {
	glog.Infof("Chunks: %d", c.chunks)
	glog.Infof("Samples: %d", c.samples)
	glog.Infof("Avg. chunk size: %d", c.samples/c.chunks)
	glog.Infof("float32 values: %d", c.float32Values)
	glog.Infof("intValues: %d (%.1f%%)", c.intValues, 100*float64(c.intValues)/float64(c.samples))
	glog.Infof("int8Deltas: %d (%.1f%%)", c.int8Deltas, 100*float64(c.int8Deltas)/float64(c.samples))
	glog.Infof("int16Deltas: %d (%.1f%%)", c.int16Deltas, 100*float64(c.int16Deltas)/float64(c.samples))
	glog.Infof("int32Deltas: %d (%.1f%%)", c.int32Deltas, 100*float64(c.int32Deltas)/float64(c.samples))
	glog.Infof("int64Deltas: %d (%.1f%%)", c.int64Deltas, 100*float64(c.int64Deltas)/float64(c.samples))
	glog.Infof("Total: %d (100%%)", c.uncompressedBytes)
	for algo, _ := range c.compressors {
		glog.Infof("%s: %d (%.1f%%)", algo, c.compressedBytes[algo], 100*float64(c.compressedBytes[algo])/float64(c.uncompressedBytes))
	}
}

var compressors = map[string]compressFn{
	"gzip": func(v []byte) int {
		var b bytes.Buffer
		w, err := gzip.NewWriterLevel(&b, gzip.BestCompression)
		if err != nil {
			glog.Fatal(err)
		}
		w.Write(v)
		w.Flush()
		w.Close()
		return b.Len()
	},
	"flate": func(v []byte) int {
		var b bytes.Buffer
		w, err := flate.NewWriter(&b, flate.BestCompression)
		if err != nil {
			glog.Fatal(err)
		}
		w.Write(v)
		w.Flush()
		w.Close()
		return b.Len()
	},
	"lzw": func(v []byte) int {
		var b bytes.Buffer
		w := lzw.NewWriter(&b, lzw.MSB, 8)
		w.Write(v)
		w.Close()
		return b.Len()
	},
	"snappy": func(v []byte) int {
		c, err := snappy.Encode(nil, v)
		if err != nil {
			glog.Fatal(err)
		}
		return len(c)
	},
	"zlib": func(v []byte) int {
		var b bytes.Buffer
		w, err := zlib.NewWriterLevel(&b, zlib.BestCompression)
		if err != nil {
			glog.Fatal(err)
		}
		w.Write(v)
		w.Flush()
		w.Close()
		return b.Len()
	},
	"bzip2": func(v []byte) int {
		f, err := ioutil.TempFile("/tmp", "bzip2_test_")
		if err != nil {
			glog.Fatal(err)
		}
		_, err = f.Write(v)
		if err != nil {
			glog.Fatal(err)
		}
		tmpName := f.Name()
		f.Close()

		cmd := exec.Command("bzip2", "--best", tmpName)
		if err = cmd.Run(); err != nil {
			glog.Fatal(err)
		}

		tmpBzip2Name := tmpName + ".bz2"
		fi, err := os.Stat(tmpBzip2Name)
		if err != nil {
			glog.Fatal(err)
		}

		if err := os.Remove(tmpBzip2Name); err != nil {
			glog.Fatal(err)
		}
		return int(fi.Size())
	},
}

func main() {
	flag.Parse()

	if storageRoot == nil || *storageRoot == "" {
		glog.Fatal("Must provide a path...")
	}

	persistence, err := tiered.NewLevelDBPersistence(*storageRoot)
	if err != nil {
		glog.Fatal(err)
	}
	defer persistence.Close()

	c := &SamplesCompressor{
		compressors:     compressors,
		compressedBytes: make(map[string]int),
	}

	entire, err := persistence.MetricSamples.ForEach(&tiered.MetricSamplesDecoder{}, &tiered.AcceptAllFilter{}, c)
	if err != nil {
		glog.Fatal("Error compressing samples: ", err)
	}
	if !entire {
		glog.Fatal("Didn't scan entire corpus")
	}
	c.Report()
}
