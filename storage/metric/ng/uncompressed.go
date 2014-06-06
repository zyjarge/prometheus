package storage_ng

import (
	"encoding/binary"
	"io"
	"math"

	clientmodel "github.com/prometheus/client_golang/model"

	"github.com/prometheus/prometheus/storage/metric"
)

const (
	uncompressedSampleSize = 16
)

type uncompressedChunk struct {
	buf []byte
}

func newUncompressedChunk() chunk {
	return &uncompressedChunk{
		buf: chunkBufs.Get(),
	}
}

// TODO: remove?
func (c *uncompressedChunk) values() <-chan *metric.SamplePair {
	n := len(c.buf) / uncompressedSampleSize

	valuesChan := make(chan *metric.SamplePair)

	go func() {
		for i := 0; i < n; i++ {
			offset := i * uncompressedSampleSize
			valuesChan <- &metric.SamplePair{
				Timestamp: clientmodel.TimestampFromUnix(int64(binary.LittleEndian.Uint64(c.buf[offset:]))),
				Value:     clientmodel.SampleValue(math.Float64frombits(binary.LittleEndian.Uint64(c.buf[offset+8:]))),
			}
		}
		close(valuesChan)
	}()
	return valuesChan
}

func (c *uncompressedChunk) add(s *metric.SamplePair) chunks {
	remainingBytes := (cap(c.buf) - len(c.buf))
	if remainingBytes < 16 {
		overflow := newUncompressedChunk().add(s)
		return append(chunks{c}, overflow...)
	}

	offset := len(c.buf)
	c.buf = c.buf[:offset+16]
	binary.LittleEndian.PutUint64(c.buf[offset:], uint64(s.Timestamp.Unix()))
	binary.LittleEndian.PutUint64(c.buf[offset+8:], math.Float64bits(float64(s.Value)))
	return chunks{c}
}

func (c *uncompressedChunk) close() {
	chunkBufs.Give(c.buf)
}

func (c *uncompressedChunk) valueAtOffset(offset int) *metric.SamplePair {
	return &metric.SamplePair{
		Timestamp: clientmodel.TimestampFromUnix(int64(binary.LittleEndian.Uint64(c.buf[offset:]))),
		Value:     clientmodel.SampleValue(math.Float64frombits(binary.LittleEndian.Uint64(c.buf[offset+8:]))),
	}
}

func (c *uncompressedChunk) firstTime() clientmodel.Timestamp {
	return c.valueAtOffset(0).Timestamp
}

func (c *uncompressedChunk) lastTime() clientmodel.Timestamp {
	return c.valueAtOffset(len(c.buf) - uncompressedSampleSize).Timestamp
}

func (c *uncompressedChunk) marshal(io.Writer) error {
	return nil
}

type uncompressedChunkIterator struct {
	// ...
}

func (c *uncompressedChunk) newIterator() chunkIterator {
	return &uncompressedChunkIterator{}
}

func (it *uncompressedChunkIterator) getValueAtTime(t clientmodel.Timestamp) metric.Values {
	return nil
}

func (it *uncompressedChunkIterator) getBoundaryValues(t metric.Interval) metric.Values {
	return nil
}

func (it *uncompressedChunkIterator) getRangeValues(t metric.Interval) metric.Values {
	return nil
}

func (it *uncompressedChunkIterator) contains(t clientmodel.Timestamp) bool {
	return false
}
