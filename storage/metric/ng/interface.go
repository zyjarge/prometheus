package storage_ng

import (
	"time"

	clientmodel "github.com/prometheus/client_golang/model"

	"github.com/prometheus/prometheus/storage/metric"
)

type Storage interface {
	// Store a group of samples.
	AppendSamples(clientmodel.Samples)
	// Preloads data, subject to a timeout.
	PreloadData(b *PreloadRequestBuilder, deadline time.Duration) error
	// Releases data.
	ReleaseData(b *PreloadRequestBuilder)
	// Get all of the metric fingerprints that are associated with the
	// provided label matchers.
	GetFingerprintsForLabelMatchers(metric.LabelMatchers) clientmodel.Fingerprints
	// Get all of the label values that are associated with a given label name.
	GetLabelValuesForLabelName(clientmodel.LabelName) clientmodel.LabelValues
	// Get the metric associated with the provided fingerprint.
	GetMetricForFingerprint(*clientmodel.Fingerprint) clientmodel.Metric
	// Get all label values that are associated with a given label name.
	GetAllValuesForLabel(clientmodel.LabelName) clientmodel.LabelValues
	// Construct an iterator for a given fingerprint.
	NewIterator(*clientmodel.Fingerprint) SeriesIterator
	// Run the request-serving and maintenance loop.
	Serve()
	// Close the MetricsStorage and releases all resources.
	Close()
}

type SeriesIterator interface {
	// Get the two values that are immediately adjacent to a given time.
	GetValueAtTime(clientmodel.Timestamp) metric.Values
	// Get the boundary values of an interval: the first value older than
	// the interval start, and the first value younger than the interval
	// end.
	GetBoundaryValues(metric.Interval) metric.Values
	// Get all values contained within a provided interval.
	GetRangeValues(metric.Interval) metric.Values
}

type Persistence interface {
	Persist(*clientmodel.Fingerprint, chunk) error
}

type Closer interface {
	// Close cleans up any used resources.
	Close()
}

// TODO: needs a real closer.
type NopCloser struct{}

func (c NopCloser) Close() {}
