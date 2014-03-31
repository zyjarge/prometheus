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

package remote

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	result  = "result"
	success = "success"
	failure = "failure"
	dropped = "dropped"

	facet     = "facet"
	occupancy = "occupancy"
	capacity  = "capacity"
)

var (
	sentSamplesCount        = prometheus.NewCounter()
	receivedSamplesCount    = prometheus.NewCounter()
	sentBatchesCount        = prometheus.NewCounter()
	receivedTimeSeriesCount = prometheus.NewCounter()
	sendLatency             = prometheus.NewDefaultHistogram()
	receiveLatency          = prometheus.NewDefaultHistogram()
	queueSize               = prometheus.NewGauge()
)

func recordSendOutcome(duration time.Duration, sampleCount int, err error) {
	labels := map[string]string{result: success}
	if err != nil {
		labels[result] = failure
	}

	sentSamplesCount.IncrementBy(labels, float64(sampleCount))
	sentBatchesCount.Increment(labels)
	ms := float64(duration / time.Millisecond)
	sendLatency.Add(labels, ms)
}

func RecordReceiveOutcome(duration time.Duration, sampleCount int, err error) {
	labels := map[string]string{result: success}
	if err != nil {
		labels[result] = failure
	}

	receivedSamplesCount.IncrementBy(labels, float64(sampleCount))
	receivedTimeSeriesCount.Increment(labels)
	ms := float64(duration / time.Millisecond)
	receiveLatency.Add(labels, ms)
}

func init() {
	prometheus.Register("prometheus_remote_tsdb_sent_samples_total", "Total number of samples processed to be sent to remote TSDB.", prometheus.NilLabels, sentSamplesCount)
	prometheus.Register("prometheus_remote_tsdb_received_samples_total", "Total number of samples received from remote TSDB.", prometheus.NilLabels, receivedSamplesCount)
	prometheus.Register("prometheus_remote_tsdb_sent_batches_total", "Total number of sample batches processed to be sent to remote TSDB.", prometheus.NilLabels, sentBatchesCount)
	prometheus.Register("prometheus_remote_tsdb_received_time_series_total", "Total number of time series received from remote TSDB.", prometheus.NilLabels, receivedTimeSeriesCount)
	prometheus.Register("prometheus_remote_tsdb_send_latency_ms", "Latency quantiles for sending samples to the remote TSDB in milliseconds.", prometheus.NilLabels, sendLatency)
	prometheus.Register("prometheus_remote_tsdb_receive_latency_ms", "Latency quantiles for receiving samples from the remote TSDB in milliseconds.", prometheus.NilLabels, receiveLatency)
	prometheus.Register("prometheus_remote_tsdb_queue_size_total", "The size and capacity of the queue of samples to be sent to the remote TSDB.", prometheus.NilLabels, queueSize)
}
