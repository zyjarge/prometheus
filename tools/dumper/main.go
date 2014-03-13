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

// Dumper is responsible for dumping all samples along with metadata contained
// in a given Prometheus metrics storage. It prints samples in unquoted CSV
// format, with commas as field separators:
//
// <fingerprint>,<chunk_first_time>,<chunk_last_time>,<chunk_sample_count>,<chunk_index>,<timestamp>,<value>
package main

import (
	"encoding/csv"
	"flag"
	//"fmt"
	"os"
	//"strconv"

	"github.com/golang/glog"
	idb "github.com/influxdb/influxdb-go"
	clientmodel "github.com/prometheus/client_golang/model"

	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/storage/metric"
)

var (
	storageRoot   = flag.String("storage.root", "", "The path to the storage root for Prometheus.")
	dieOnBadChunk = flag.Bool("dieOnBadChunk", false, "Whether to die upon encountering a bad chunk.")
)

type SamplesDumper struct {
	*csv.Writer
	influxdb           *idb.Client
	storage            metric.MetricPersistence
	currentFingerprint *clientmodel.Fingerprint
	currentMetric      clientmodel.Metric
	columns            []string
	values             []interface{}
}

func (d *SamplesDumper) Operate(key, value interface{}) *storage.OperatorError {
	sampleKey := key.(*metric.SampleKey)
	if d.currentFingerprint == nil || !sampleKey.Fingerprint.Equal(d.currentFingerprint) {
		d.currentFingerprint = sampleKey.Fingerprint
		if metric, err := d.storage.GetMetricForFingerprint(d.currentFingerprint); err != nil {
			glog.Fatalf("error getting metric for fingerprint %s: %s", sampleKey.Fingerprint, err)
		} else {
			d.currentMetric = metric
		}
		d.columns = make([]string, 0, len(d.currentMetric)+1)
		d.values = make([]interface{}, 0, len(d.currentMetric)+1)
		for col, val := range d.currentMetric {
			if col != clientmodel.MetricNameLabel {
				d.columns = append(d.columns, string(col))
				d.values = append(d.values, string(val))
			}
		}
		d.columns = append(d.columns, []string{"time", "value"}...)
		d.values = append(d.values, []interface{}{0, 0}...)
	}
	if *dieOnBadChunk && sampleKey.FirstTimestamp.After(sampleKey.LastTimestamp) {
		glog.Fatalf("Chunk: First time (%v) after last time (%v): %v\n", sampleKey.FirstTimestamp.Unix(), sampleKey.LastTimestamp.Unix(), sampleKey)
	}
	for _, sample := range value.(metric.Values) {
		if *dieOnBadChunk && (sample.Timestamp.Before(sampleKey.FirstTimestamp) || sample.Timestamp.After(sampleKey.LastTimestamp)) {
			glog.Fatalf("Sample not within chunk boundaries: chunk FirstTimestamp (%v), chunk LastTimestamp (%v) vs. sample Timestamp (%v)\n", sampleKey.FirstTimestamp.Unix(), sampleKey.LastTimestamp.Unix(), sample.Timestamp)
		}

		d.values[len(d.values)-2] = sample.Timestamp.Unix()
		d.values[len(d.values)-1] = float64(sample.Value)

		series := idb.Series{
			Name:    string(d.currentMetric[clientmodel.MetricNameLabel]),
			Columns: d.columns,
			Points:  [][]interface{}{d.values},
		}
		glog.Info(series)
		if err := d.influxdb.WriteSeries([]*idb.Series{&series}); err != nil {
			glog.Fatal("error writing to InfluxDB:", err)
		}
		/*
			d.Write([]string{
				d.currentMetric.String(),
				sampleKey.Fingerprint.String(),
				strconv.FormatInt(sampleKey.FirstTimestamp.Unix(), 10),
				strconv.FormatInt(sampleKey.LastTimestamp.Unix(), 10),
				strconv.FormatUint(uint64(sampleKey.SampleCount), 10),
				strconv.Itoa(i),
				strconv.FormatInt(sample.Timestamp.Unix(), 10),
				fmt.Sprintf("%v", sample.Value),
			})
			if err := d.Error(); err != nil {
				return &storage.OperatorError{
					Error:       err,
					Continuable: false,
				}
			}
		*/
	}
	return nil
}

func main() {
	flag.Parse()

	if storageRoot == nil || *storageRoot == "" {
		glog.Fatal("Must provide a path...")
	}

	persistence, err := metric.NewLevelDBMetricPersistence(*storageRoot)
	if err != nil {
		glog.Fatal(err)
	}
	defer persistence.Close()

	dumper := &SamplesDumper{
		Writer:   csv.NewWriter(os.Stdout),
		influxdb: newInfluxDB(),
		storage:  persistence,
	}

	entire, err := persistence.MetricSamples.ForEach(&metric.MetricSamplesDecoder{}, &metric.AcceptAllFilter{}, dumper)
	if err != nil {
		glog.Fatal("Error dumping samples: ", err)
	}
	if !entire {
		glog.Fatal("Didn't scan entire corpus")
	}
	dumper.Flush()
	if err = dumper.Error(); err != nil {
		glog.Fatal("Error flushing CSV: ", err)
	}
}
