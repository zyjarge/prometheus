package storage_ng

import (
	"io"

	clientmodel "github.com/prometheus/client_golang/model"

	"github.com/prometheus/prometheus/storage/metric"
)

type chunks []chunk

type chunk interface {
	add(*metric.SamplePair) chunks
	firstTime() clientmodel.Timestamp
	lastTime() clientmodel.Timestamp
	newIterator() chunkIterator
	marshal(io.Writer) error
	close()

	// TODO: remove?
	values() <-chan *metric.SamplePair
}

type chunkIterator interface {
	getValueAtTime(clientmodel.Timestamp) metric.Values
	getBoundaryValues(metric.Interval) metric.Values
	getRangeValues(metric.Interval) metric.Values
	contains(clientmodel.Timestamp) bool
}

func transcodeAndAdd(dst chunk, src chunk, s *metric.SamplePair) chunks {
	numTranscodes.Inc()
	defer src.close()

	head := dst
	body := chunks{}
	for v := range src.values() {
		newChunks := head.add(v)
		body = append(body, newChunks[:len(newChunks)-1]...)
		head = newChunks[len(newChunks)-1]
	}
	newChunks := head.add(s)
	body = append(body, newChunks[:len(newChunks)-1]...)
	head = newChunks[len(newChunks)-1]
	return append(body, head)
}
