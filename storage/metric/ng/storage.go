package storage_ng

import (
	"sync"
	"time"

	"github.com/golang/glog"

	clientmodel "github.com/prometheus/client_golang/model"

	"github.com/prometheus/prometheus/storage/metric"
	"github.com/prometheus/prometheus/utility"
)

const persistQueueCap = 1024

type memorySeriesStorage struct {
	mtx sync.RWMutex

	fingerprintToSeries     map[clientmodel.Fingerprint]*memorySeries
	labelPairToFingerprints map[metric.LabelPair]utility.Set
	labelNameToLabelValues  map[clientmodel.LabelName]utility.Set

	persistQueue chan *persistRequest
	persistence  Persistence
}

func NewMemorySeriesStorage(p Persistence) *memorySeriesStorage { // TODO: change to return Storage?
	return &memorySeriesStorage{
		fingerprintToSeries:     make(map[clientmodel.Fingerprint]*memorySeries),
		labelPairToFingerprints: make(map[metric.LabelPair]utility.Set),
		labelNameToLabelValues:  make(map[clientmodel.LabelName]utility.Set),

		persistQueue: make(chan *persistRequest, persistQueueCap),
		persistence:  p,
	}
}

type persistRequest struct {
	fingerprint *clientmodel.Fingerprint
	chunkDesc   *chunkDesc
}

func (s *memorySeriesStorage) AppendSamples(samples clientmodel.Samples) {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	for _, sample := range samples {
		s.appendSample(sample)
	}

	numSamples.Add(float64(len(samples)))
}

func (s *memorySeriesStorage) appendSample(sample *clientmodel.Sample) {
	series := s.getOrCreateSeries(sample.Metric)
	series.add(&metric.SamplePair{
		Value:     sample.Value,
		Timestamp: sample.Timestamp,
	}, s.persistQueue)
}

func (s *memorySeriesStorage) getOrCreateSeries(m clientmodel.Metric) *memorySeries {
	fp := clientmodel.Fingerprint{}
	fp.LoadFromMetric(m)
	series, ok := s.fingerprintToSeries[fp]

	if !ok {
		series = newMemorySeries(m)
		s.fingerprintToSeries[fp] = series
		numSeries.Set(float64(len(s.fingerprintToSeries)))

		for k, v := range m {
			labelPair := metric.LabelPair{
				Name:  k,
				Value: v,
			}

			fps, ok := s.labelPairToFingerprints[labelPair]
			if !ok {
				fps = utility.Set{}
				s.labelPairToFingerprints[labelPair] = fps
			}
			fps.Add(fp)

			values, ok := s.labelNameToLabelValues[k]
			if !ok {
				values = utility.Set{}
				s.labelNameToLabelValues[k] = values
			}
			values.Add(v)
		}
	}
	return series
}

// TODO needs to be handled via channels, etc.
func (s *memorySeriesStorage) Close() {
	for _, series := range s.fingerprintToSeries {
		series.close()
	}
	s.fingerprintToSeries = nil
}

func (s *memorySeriesStorage) NewIterator(fp *clientmodel.Fingerprint) SeriesIterator {
	series, ok := s.fingerprintToSeries[*fp]
	if !ok {
		panic("requested iterator for non-existent series")
	}
	return series.newIterator()
}

func (s *memorySeriesStorage) evictMemoryChunks(ttl time.Duration) {
	for _, series := range s.fingerprintToSeries {
		series.evictOlderThan(clientmodel.TimestampFromTime(time.Now()).Add(-1 * ttl))
		// TODO: if series now empty, delete it from index? Probably not, only when
		// we also delete the last chunks from disk/LTS?
	}
}

func (s *memorySeriesStorage) persistMemoryChunks() {
	glog.Infof("Persisting %d series to disk...", len(s.fingerprintToSeries))
	for _, series := range s.fingerprintToSeries {
		func() {
			if err := series.persist(s.persistence); err != nil {
				glog.Errorf("Error marshalling series: ", err)
			}
		}()
	}
	// TODO: persist index.
	glog.Infof("Done persisting.")
}

func recordPersist(start time.Time, err error) {
	outcome := success
	if err != nil {
		outcome = failure
	}
	persistLatencies.WithLabelValues(outcome).Observe(float64(time.Since(start) / time.Millisecond))
}

func (s *memorySeriesStorage) handlePersistQueue() {
	for req := range s.persistQueue {
		// TODO: Make this thread-safe?
		persistQueueLength.Set(float64(len(s.persistQueue)))

		//glog.Info("Persist request: ", *req.fingerprint)
		if req.chunkDesc.lastPersist.After(req.chunkDesc.lastWrite) {
			glog.Info("Skipping.")
			continue
		}
		start := time.Now()
		err := s.persistence.Persist(req.fingerprint, req.chunkDesc.chunk)
		recordPersist(start, err)
		if err != nil {
			glog.Error("Error persisting chunk: ", err)
			continue
		}
		req.chunkDesc.lastPersist = time.Now()
	}
}

func (s *memorySeriesStorage) Serve() {
	evictMemoryTicker := time.NewTicker(time.Minute) //t.evictMemoryInterval)
	defer evictMemoryTicker.Stop()

	go s.handlePersistQueue()

	for {
		select {
		case <-evictMemoryTicker.C:
			s.evictMemoryChunks(time.Hour) //ttl)
		}
	}
	/*
			s.mtx.Lock()
			if s.state != storageStarting {
				panic("Illegal State: Attempted to restart memorySeriesStorage.")
			}

			s.state = storageServing
			s.mtx.Unlock()

		persistTicker := time.NewTicker(time.Minute) //t.flushMemoryInterval)
		defer flushMemoryTicker.Stop()

			for {
				select {
				case <-persistTicker.C:
					go s.persist()
				case viewRequest := <-t.ViewQueue:
					go s.buildView(viewRequest)
				case drainingDone := <-t.draining:
					s.flush()
				}
			}
	*/
}

func (s *memorySeriesStorage) PreloadData(b *PreloadRequestBuilder, deadline time.Duration) error {
	return nil
}

func (s *memorySeriesStorage) ReleaseData(b *PreloadRequestBuilder) {
}

func (s *memorySeriesStorage) GetFingerprintsForLabelMatchers(labelMatchers metric.LabelMatchers) clientmodel.Fingerprints {
	s.mtx.RLock()
	defer s.mtx.RUnlock()

	sets := []utility.Set{}
	for _, matcher := range labelMatchers {
		switch matcher.Type {
		case metric.Equal:
			set, ok := s.labelPairToFingerprints[metric.LabelPair{
				Name:  matcher.Name,
				Value: matcher.Value,
			}]
			if !ok {
				return nil
			}
			sets = append(sets, set)
		default:
			values := s.getLabelValuesForLabelName(matcher.Name)
			matches := matcher.Filter(values)
			if len(matches) == 0 {
				return nil
			}
			set := utility.Set{}
			for _, v := range matches {
				subset, ok := s.labelPairToFingerprints[metric.LabelPair{
					Name:  matcher.Name,
					Value: v,
				}]
				if !ok {
					return nil
				}
				for fp := range subset {
					set.Add(fp)
				}
			}
			sets = append(sets, set)
		}
	}

	setCount := len(sets)
	if setCount == 0 {
		return nil
	}

	base := sets[0]
	for i := 1; i < setCount; i++ {
		base = base.Intersection(sets[i])
	}

	fingerprints := clientmodel.Fingerprints{}
	for _, e := range base.Elements() {
		fingerprint := e.(clientmodel.Fingerprint)
		fingerprints = append(fingerprints, &fingerprint)
	}

	return fingerprints
}

func (s *memorySeriesStorage) GetLabelValuesForLabelName(labelName clientmodel.LabelName) clientmodel.LabelValues {
	s.mtx.RLock()
	defer s.mtx.RUnlock()

	return s.getLabelValuesForLabelName(labelName)
}

func (s *memorySeriesStorage) getLabelValuesForLabelName(labelName clientmodel.LabelName) clientmodel.LabelValues {
	set, ok := s.labelNameToLabelValues[labelName]
	if !ok {
		return nil
	}

	values := make(clientmodel.LabelValues, 0, len(set))
	for e := range set {
		val := e.(clientmodel.LabelValue)
		values = append(values, val)
	}
	return values
}

func (s *memorySeriesStorage) GetMetricForFingerprint(f *clientmodel.Fingerprint) clientmodel.Metric {
	s.mtx.RLock()
	defer s.mtx.RUnlock()

	series, ok := s.fingerprintToSeries[*f]
	if !ok {
		return nil
	}

	metric := clientmodel.Metric{}
	for label, value := range series.metric {
		metric[label] = value
	}

	return metric
}

func (s *memorySeriesStorage) GetAllValuesForLabel(labelName clientmodel.LabelName) clientmodel.LabelValues {
	s.mtx.RLock()
	defer s.mtx.RUnlock()

	var values clientmodel.LabelValues
	valueSet := map[clientmodel.LabelValue]struct{}{}
	for _, series := range s.fingerprintToSeries {
		if value, ok := series.metric[labelName]; ok {
			if _, ok := valueSet[value]; !ok {
				values = append(values, value)
				valueSet[value] = struct{}{}
			}
		}
	}

	return values
}
