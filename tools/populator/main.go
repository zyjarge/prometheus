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
	"fmt"
	"log"
	"math"
	"net/http"
	_ "net/http/pprof"
	"os"
	"time"

	"github.com/prometheus/client_golang/model"
	"github.com/prometheus/prometheus/storage/metric"
)

// Commandline flags.
var (
	storageRoot = flag.String("storage.root", "/tmp/metrics", "Base path for metrics storage.")

	// Sample data generation flags.
	deleteStorage          = flag.Bool("deleteStorage", false, "Whether to remove the storage path before exiting.")
	numTimeseries          = flag.Int("numTimeseries", 100, "Number of timeseries to generate.")
	startTimeseries        = flag.Int("startTimeseries", 0, "Start at this timeseries number.")
	numValuesPerTimeseries = flag.Int("numValuesPerTimeseries", 10000, "Number of data points per timeseries to generate.")
	numLabels              = flag.Int("numLabels", 5, "Number of labels to attach to timeseries.")
	valueIntervalSeconds   = flag.Int("valueIntervalSeconds", 15, "Time in seconds between generated data points.")
)

// Creates a group timeseries with the format:
//
// metric_0{label0="value", label1="value", ...}
// metric_1{label0="value", label1="value", ...}
// metric_2{label0="value", label1="value", ...}
// ...
//
// Only the metric name is varied between timeseries, since label variance
// should otherwise not matter for lookup speeds (all labels are hashed
// together, and this fingerprint will be completely different for all these
// timeseries). The non-varying labels on the timeseries are mainly added to
// simulate real fingerprinting times, etc.
//
// The number of static labels is determined by -numLabels, while the number of
// timeseries is set by -numTimeseries.
func appendSamples(storage *metric.TieredStorage, endTime time.Time) {
	interval := time.Duration(*valueIntervalSeconds) * time.Second
	startTime := endTime.Add(-interval * time.Duration(*numValuesPerTimeseries))
	for ts := *startTimeseries; ts < *numTimeseries; ts++ {
		buildStart := time.Now()
		metric := model.Metric{}

		for i := 0; i < *numLabels; i++ {
			metric[model.LabelName(fmt.Sprintf("label%d", i))] = "value"
		}

		metric["name"] = model.LabelValue(fmt.Sprintf("metric_%d", ts))
		x := float64(ts) / 10
		samples := model.Samples{}
		for t := startTime; t.Before(endTime); t = t.Add(interval) {
			samples = append(samples, &model.Sample{
				Metric:    metric,
				Value:     model.SampleValue(math.Sin(x)),
				Timestamp: model.TimestampFromTime(t),
			})
			x += 0.05
		}
		fmt.Println("Build:", time.Since(buildStart))

		appendStart := time.Now()
		storage.AppendSamples(samples)
		fmt.Println("Append:", time.Since(appendStart))

		if ts%100 == 0 {
			fmt.Println("At timeseries", ts)
			flushStart := time.Now()
			storage.Flush()
			fmt.Println("Flush", time.Since(flushStart))
		}
	}
	fmt.Printf("Flushing...\n")
	storage.Flush()
	fmt.Printf("Finished flushing...\n")
}

func main() {
	flag.Parse()
	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	err := os.MkdirAll(*storageRoot, 0755)
	if err != nil {
		log.Fatalf("Error creating storage directory: %s", err)
	}
	ts, err := metric.NewTieredStorage(50000, 100, 15*time.Minute, 10*time.Minute, *storageRoot)
	if err != nil {
		log.Fatalf("Error opening storage: %s", err)
	}
	defer func() {
		if *deleteStorage {
			os.RemoveAll(*storageRoot)
		}
	}()
	storageStarted := make(chan bool)
	go ts.Serve(storageStarted)
	<-storageStarted

	startTime := time.Date(2014, 1, 1, 0, 0, 0, 0, time.UTC)
	appendSamples(ts, startTime)
	fmt.Printf("Total population time: %v\n", time.Since(startTime))
	ts.Close()
}
