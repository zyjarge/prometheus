package storage_ng

import (
	"io"

	clientmodel "github.com/prometheus/client_golang/model"

	"github.com/prometheus/prometheus/storage/metric"
)

type dummyChunk struct{}

func newDummyChunk() chunk {
	return &dummyChunk{}
}

// TODO: remove?
func (c *dummyChunk) values() <-chan *metric.SamplePair {
	valuesChan := make(chan *metric.SamplePair)
	close(valuesChan)
	return valuesChan
}

func (c *dummyChunk) add(s *metric.SamplePair) chunks {
	return chunks{c}
}

func (c *dummyChunk) close() {
}

func (c *dummyChunk) valueAtOffset(offset int) *metric.SamplePair {
	return &metric.SamplePair{}
}

func (c *dummyChunk) firstTime() clientmodel.Timestamp {
	return clientmodel.Timestamp(0)
}

func (c *dummyChunk) lastTime() clientmodel.Timestamp {
	return clientmodel.Timestamp(0)
}

func (c *dummyChunk) marshal(w io.Writer) error {
	return nil
}

type dummyChunkIterator struct{}

func (c *dummyChunk) newIterator() chunkIterator {
	return &dummyChunkIterator{}
}

func (it *dummyChunkIterator) getValueAtTime(t clientmodel.Timestamp) metric.Values {
	return nil
}

func (it *dummyChunkIterator) getBoundaryValues(t metric.Interval) metric.Values {
	return nil
}

func (it *dummyChunkIterator) getRangeValues(t metric.Interval) metric.Values {
	return nil
}

func (it *dummyChunkIterator) contains(t clientmodel.Timestamp) bool {
	return false
}
