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
	"flag"
	"time"

	"github.com/golang/glog"

	clientmodel "github.com/prometheus/client_golang/model"

	"github.com/prometheus/prometheus/storage/metric"
)

// Commandline flags.
var (
	storageRoot = flag.String("storage.root", "/tmp/metrics", "Base path for metrics storage.")

	samplesQueueCapacity      = flag.Int("storage.queue.samplesCapacity", 4096, "The size of the unwritten samples queue.")
	diskAppendQueueCapacity   = flag.Int("storage.queue.diskAppendCapacity", 1000000, "The size of the queue for items that are pending writing to disk.")
	memoryAppendQueueCapacity = flag.Int("storage.queue.memoryAppendCapacity", 10000, "The size of the queue for items that are pending writing to memory.")

	groupSize = flag.Int("compact.groupSize", 5000, "The minimum group size for samples.")

	instant          = flag.Int("compact.instant", int(time.Date(2014, 1, 1, 0, 0, 0, 0, time.UTC).Unix()), "The relative inclusiveness of samples.")
	ageInclusiveness = flag.Duration("compact.ageInclusiveness", time.Minute, "The relative inclusiveness of samples.")

	arenaFlushInterval = flag.Duration("arena.flushInterval", 15*time.Minute, "The period at which the in-memory arena is flushed to disk.")
	arenaTTL           = flag.Duration("arena.ttl", 10*time.Minute, "The relative age of values to purge to disk from memory.")
)

type prometheus struct {
	stopBackgroundOperations chan bool
	storage                  *metric.TieredStorage
	curationState            metric.CurationStateUpdater
}

func (p *prometheus) compact(olderThan time.Duration, groupSize int) error {
	processor := metric.NewCompactionProcessor(&metric.CompactionProcessorOptions{
		MaximumMutationPoolBatch: groupSize * 3,
		MinimumGroupSize:         groupSize,
	})
	defer processor.Close()

	curator := metric.NewCurator(&metric.CuratorOptions{
		Stop: make(chan bool, 1),

		ViewQueue: p.storage.ViewQueue,
	})
	defer curator.Close()

	return curator.Run(olderThan, clientmodel.TimestampFromUnix(int64(*instant)), processor, p.storage.DiskStorage.CurationRemarks, p.storage.DiskStorage.MetricSamples, p.storage.DiskStorage.MetricHighWatermarks, p.curationState)
}

type nopCurationStateUpdater struct{}

func (n *nopCurationStateUpdater) UpdateCurationState(*metric.CurationState) {}

func main() {
	flag.Parse()

	ts, err := metric.NewTieredStorage(uint(*diskAppendQueueCapacity), 100, *arenaFlushInterval, *arenaTTL, *storageRoot)
	if err != nil {
		glog.Fatal("Error opening storage: ", err)
	}

	prometheus := &prometheus{
		storage:       ts,
		curationState: &nopCurationStateUpdater{},
	}

	storageStarted := make(chan bool)
	go ts.Serve(storageStarted)
	<-storageStarted

	glog.Info("Starting body compaction...")
	err = prometheus.compact(*ageInclusiveness, *groupSize)

	if err != nil {
		glog.Error("could not compact: ", err)
	}
	glog.Info("Done")
}
