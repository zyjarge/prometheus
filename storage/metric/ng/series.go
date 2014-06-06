package storage_ng

import (
	"fmt"
	"sort"
	"time"

	clientmodel "github.com/prometheus/client_golang/model"

	"github.com/prometheus/prometheus/storage/metric"
)

type chunkDescs []chunkDesc

type chunkDesc struct {
	chunk       chunk
	refCount    int
	lastWrite   time.Time
	lastPersist time.Time
}

func (cd *chunkDesc) add(s *metric.SamplePair) chunks {
	return cd.chunk.add(s)
}

func (cd *chunkDesc) okToFlush() bool {
	return cd.refCount == 0 && true //c.lastPersist.After(time.Time{})
}

func (cd *chunkDesc) close() {
	cd.chunk.close()
}

type memorySeries struct {
	metric clientmodel.Metric
	// Sorted by start time, no overlapping chunk ranges allowed.
	chunkDescs chunkDescs
}

type memorySeriesIterator struct {
	chunkIt chunkIterator
	chunks  chunks
}

func (s *memorySeries) newIterator() SeriesIterator {
	chunks := make(chunks, 0, len(s.chunkDescs))
	for _, cd := range s.chunkDescs {
		chunks = append(chunks, cd.chunk)
	}

	return &memorySeriesIterator{
		chunks: chunks,
	}
}

func (s *memorySeries) head() *chunkDesc {
	return &s.chunkDescs[len(s.chunkDescs)-1]
}

func (s *memorySeries) values() metric.Values {
	var values metric.Values
	for _, cd := range s.chunkDescs {
		for sample := range cd.chunk.values() {
			values = append(values, *sample)
		}
	}
	return values
}

func newMemorySeries(m clientmodel.Metric) *memorySeries {
	return &memorySeries{
		metric: m,
		// TODO: should we set this to nil initially and only create a chunk when
		// adding? But right now, we also only call newMemorySeries when adding, so
		// it turns out to be the same.
		chunkDescs: chunkDescs{chunkDesc{chunk: newDeltaEncodedChunk(d1, d1, true)}},
		//chunks: chunkDescs{chunkDesc{chunk: newDummyChunk()}},
	}
}

func (s *memorySeries) add(v *metric.SamplePair, persistQueue chan *persistRequest) {
	now := time.Now()
	chunks := s.head().add(v)

	s.head().chunk = chunks[0]
	s.head().lastWrite = now
	if len(chunks) > 1 {
		fp := &clientmodel.Fingerprint{}
		fp.LoadFromMetric(s.metric)

		queuePersist := func(cd *chunkDesc) {
			persistQueue <- &persistRequest{
				fingerprint: fp,
				chunkDesc:   cd,
			}
		}

		queuePersist(s.head())

		for i, c := range chunks[1:] {
			cd := chunkDesc{
				chunk:     c,
				lastWrite: now,
			}
			s.chunkDescs = append(s.chunkDescs, cd)
			// The last chunk is still growing.
			if i < len(chunks[1:])-1 {
				queuePersist(&cd)
			}
		}
	}
}

func (s *memorySeries) evictOlderThan(t clientmodel.Timestamp) {
	// For now, always drop the entire range from oldest to t, but only if all of
	// the chunks in question are ok to be flushed.
	firstKeepIdx := len(s.chunkDescs)
	for i, cd := range s.chunkDescs {
		if cd.chunk.lastTime().Before(t) && cd.okToFlush() {
			cd.chunk.close()
		} else {
			firstKeepIdx = i
			break
		}
	}
	fp := clientmodel.Fingerprint{}
	fp.LoadFromMetric(s.metric)
	if firstKeepIdx > 0 {
		fmt.Printf("Dropping %s until %v (drop until time %v)\n", fp.String(), firstKeepIdx, t.Time())
	}
	s.chunkDescs = s.chunkDescs[firstKeepIdx:]
}

func (s *memorySeries) persist(p Persistence) error {
	fp := clientmodel.Fingerprint{}
	fp.LoadFromMetric(s.metric)
	for _, cd := range s.chunkDescs {
		if cd.lastPersist.After(cd.lastWrite) {
			continue
		}
		if err := p.Persist(&fp, cd.chunk); err != nil {
			return err
		}
		cd.lastPersist = time.Now()
	}
	return nil
}

func (s *memorySeries) close() {
	for _, cd := range s.chunkDescs {
		cd.close()
	}
}

func (it *memorySeriesIterator) GetValueAtTime(t clientmodel.Timestamp) metric.Values {
	// The most common case. We are iterating through a chunk.
	if it.chunkIt != nil && it.chunkIt.contains(t) {
		return it.chunkIt.getValueAtTime(t)
	}

	it.chunkIt = nil

	if len(it.chunks) == 0 {
		return nil
	}

	// Before or exactly on the first sample of the series.
	if !t.After(it.chunks[0].firstTime()) {
		// return first value of first chunk
		return it.chunks[0].newIterator().getValueAtTime(t)
	}
	// After or exactly on the last sample of the series.
	if !t.Before(it.chunks[len(it.chunks)-1].lastTime()) {
		// return last value of last chunk
		return it.chunks[len(it.chunks)-1].newIterator().getValueAtTime(t)
	}

	// Find first chunk where lastTime() is after or equal to t.
	i := sort.Search(len(it.chunks), func(i int) bool {
		return !it.chunks[i].lastTime().Before(t)
	})
	if i == len(it.chunks) {
		panic("out of bounds")
	}

	if t.Before(it.chunks[i].firstTime()) {
		// We ended up between two chunks.
		return metric.Values{
			it.chunks[i-1].newIterator().getValueAtTime(t)[0],
			it.chunks[i].newIterator().getValueAtTime(t)[0],
		}
	} else {
		// We ended up in the middle of a chunk. We might stay there for a while,
		// so save it as the current chunk iterator.
		it.chunkIt = it.chunks[i].newIterator()
		return it.chunkIt.getValueAtTime(t)
	}
}

func (it *memorySeriesIterator) GetBoundaryValues(in metric.Interval) metric.Values {
	return nil
}

func (it *memorySeriesIterator) GetRangeValues(in metric.Interval) metric.Values {
	// Find the first relevant chunk.
	i := sort.Search(len(it.chunks), func(i int) bool {
		return !it.chunks[i].lastTime().Before(in.OldestInclusive)
	})
	values := metric.Values{}
	for _, c := range it.chunks[i:] {
		if c.firstTime().After(in.NewestInclusive) {
			break
		}
		// TODO: actually reuse an iterator between calls if we get multiple ranges
		// from the same chunk.
		values = append(values, c.newIterator().getRangeValues(in)...)
	}
	return values
}
