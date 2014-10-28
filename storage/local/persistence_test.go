// Copyright 2014 Prometheus Team
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

package local

import (
	"reflect"
	"testing"

	clientmodel "github.com/prometheus/client_golang/model"

	"github.com/prometheus/prometheus/storage/local/codable"
	"github.com/prometheus/prometheus/storage/local/index"
	"github.com/prometheus/prometheus/storage/metric"
	"github.com/prometheus/prometheus/utility/test"
)

var (
	m1 = clientmodel.Metric{"label": "value1"}
	m2 = clientmodel.Metric{"label": "value2"}
	m3 = clientmodel.Metric{"label": "value3"}
)

func newTestPersistence(t *testing.T) (*persistence, test.Closer) {
	dir := test.NewTemporaryDirectory("test_persistence", t)
	p, err := newPersistence(dir.Path(), 1024)
	if err != nil {
		dir.Close()
		t.Fatal(err)
	}
	return p, test.NewCallbackCloser(func() {
		p.close()
		dir.Close()
	})
}

func buildTestChunks() map[clientmodel.Fingerprint][]chunk {
	fps := clientmodel.Fingerprints{
		m1.Fingerprint(),
		m2.Fingerprint(),
		m3.Fingerprint(),
	}
	fpToChunks := map[clientmodel.Fingerprint][]chunk{}

	for _, fp := range fps {
		fpToChunks[fp] = make([]chunk, 0, 10)
		for i := 0; i < 10; i++ {
			fpToChunks[fp] = append(fpToChunks[fp], newDeltaEncodedChunk(d1, d1, true).add(&metric.SamplePair{
				Timestamp: clientmodel.Timestamp(i),
				Value:     clientmodel.SampleValue(fp),
			})[0])
		}
	}
	return fpToChunks
}

func chunksEqual(c1, c2 chunk) bool {
	values2 := c2.values()
	for v1 := range c1.values() {
		v2 := <-values2
		if !v1.Equal(v2) {
			return false
		}
	}
	return true
}

func TestPersistAndLoadChunks(t *testing.T) {
	p, closer := newTestPersistence(t)
	defer closer.Close()

	fpToChunks := buildTestChunks()

	for fp, chunks := range fpToChunks {
		for i, c := range chunks {
			index, err := p.persistChunk(fp, c)
			if err != nil {
				t.Fatal(err)
			}
			if i != index {
				t.Errorf("Want chunk index %d, got %d.", i, index)
			}
		}
	}

	for fp, expectedChunks := range fpToChunks {
		indexes := make([]int, 0, len(expectedChunks))
		for i := range expectedChunks {
			indexes = append(indexes, i)
		}
		actualChunks, err := p.loadChunks(fp, indexes, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, i := range indexes {
			if !chunksEqual(expectedChunks[i], actualChunks[i]) {
				t.Errorf("%d. Chunks not equal.", i)
			}
		}
		// Load all chunk descs.
		actualChunkDescs, err := p.loadChunkDescs(fp, 10)
		if len(actualChunkDescs) != 10 {
			t.Errorf("Got %d chunkDescs, want %d.", len(actualChunkDescs), 10)
		}
		for i, cd := range actualChunkDescs {
			if cd.firstTime() != clientmodel.Timestamp(i) || cd.lastTime() != clientmodel.Timestamp(i) {
				t.Errorf(
					"Want ts=%v, got firstTime=%v, lastTime=%v.",
					i, cd.firstTime(), cd.lastTime(),
				)
			}

		}
		// Load chunk descs partially.
		actualChunkDescs, err = p.loadChunkDescs(fp, 5)
		if len(actualChunkDescs) != 5 {
			t.Errorf("Got %d chunkDescs, want %d.", len(actualChunkDescs), 5)
		}
		for i, cd := range actualChunkDescs {
			if cd.firstTime() != clientmodel.Timestamp(i) || cd.lastTime() != clientmodel.Timestamp(i) {
				t.Errorf(
					"Want ts=%v, got firstTime=%v, lastTime=%v.",
					i, cd.firstTime(), cd.lastTime(),
				)
			}

		}
	}
}

func TestCheckpointAndLoadSeriesMapAndHeads(t *testing.T) {
	p, closer := newTestPersistence(t)
	defer closer.Close()

	fpLocker := newFingerprintLocker(10)
	sm := newSeriesMap()
	s1 := newMemorySeries(m1, true)
	s2 := newMemorySeries(m2, false)
	s3 := newMemorySeries(m3, false)
	s1.add(m1.Fingerprint(), &metric.SamplePair{1, 3.14})
	s3.add(m1.Fingerprint(), &metric.SamplePair{2, 2.7})
	s3.headChunkPersisted = true
	sm.put(m1.Fingerprint(), s1)
	sm.put(m2.Fingerprint(), s2)
	sm.put(m3.Fingerprint(), s3)

	if err := p.checkpointSeriesMapAndHeads(sm, fpLocker); err != nil {
		t.Fatal(err)
	}

	loadedSM, err := p.loadSeriesMapAndHeads()
	if err != nil {
		t.Fatal(err)
	}
	if loadedSM.length() != 2 {
		t.Errorf("want 2 series in map, got %d", loadedSM.length())
	}
	if loadedS1, ok := loadedSM.get(m1.Fingerprint()); ok {
		if !reflect.DeepEqual(loadedS1.metric, m1) {
			t.Errorf("want metric %v, got %v", m1, loadedS1.metric)
		}
		if !reflect.DeepEqual(loadedS1.head().chunk, s1.head().chunk) {
			t.Error("head chunks differ")
		}
		if loadedS1.chunkDescsOffset != 0 {
			t.Errorf("want chunkDescsOffset 0, got %d", loadedS1.chunkDescsOffset)
		}
		if loadedS1.headChunkPersisted {
			t.Error("headChunkPersisted is true")
		}
	} else {
		t.Errorf("couldn't find %v in loaded map", m1)
	}
	if loadedS3, ok := loadedSM.get(m3.Fingerprint()); ok {
		if !reflect.DeepEqual(loadedS3.metric, m3) {
			t.Errorf("want metric %v, got %v", m3, loadedS3.metric)
		}
		if loadedS3.head().chunk != nil {
			t.Error("head chunk not evicted")
		}
		if loadedS3.chunkDescsOffset != -1 {
			t.Errorf("want chunkDescsOffset -1, got %d", loadedS3.chunkDescsOffset)
		}
		if !loadedS3.headChunkPersisted {
			t.Error("headChunkPersisted is false")
		}
	} else {
		t.Errorf("couldn't find %v in loaded map", m1)
	}
}

type incrementalBatch struct {
	fpToMetric      index.FingerprintMetricMapping
	expectedLnToLvs index.LabelNameLabelValuesMapping
	expectedLpToFps index.LabelPairFingerprintsMapping
}

func TestIndexing(t *testing.T) {
	batches := []incrementalBatch{
		{
			fpToMetric: index.FingerprintMetricMapping{
				0: {
					clientmodel.MetricNameLabel: "metric_0",
					"label_1":                   "value_1",
				},
				1: {
					clientmodel.MetricNameLabel: "metric_0",
					"label_2":                   "value_2",
					"label_3":                   "value_3",
				},
				2: {
					clientmodel.MetricNameLabel: "metric_1",
					"label_1":                   "value_2",
				},
			},
			expectedLnToLvs: index.LabelNameLabelValuesMapping{
				clientmodel.MetricNameLabel: codable.LabelValueSet{
					"metric_0": struct{}{},
					"metric_1": struct{}{},
				},
				"label_1": codable.LabelValueSet{
					"value_1": struct{}{},
					"value_2": struct{}{},
				},
				"label_2": codable.LabelValueSet{
					"value_2": struct{}{},
				},
				"label_3": codable.LabelValueSet{
					"value_3": struct{}{},
				},
			},
			expectedLpToFps: index.LabelPairFingerprintsMapping{
				metric.LabelPair{
					Name:  clientmodel.MetricNameLabel,
					Value: "metric_0",
				}: codable.FingerprintSet{0: struct{}{}, 1: struct{}{}},
				metric.LabelPair{
					Name:  clientmodel.MetricNameLabel,
					Value: "metric_1",
				}: codable.FingerprintSet{2: struct{}{}},
				metric.LabelPair{
					Name:  "label_1",
					Value: "value_1",
				}: codable.FingerprintSet{0: struct{}{}},
				metric.LabelPair{
					Name:  "label_1",
					Value: "value_2",
				}: codable.FingerprintSet{2: struct{}{}},
				metric.LabelPair{
					Name:  "label_2",
					Value: "value_2",
				}: codable.FingerprintSet{1: struct{}{}},
				metric.LabelPair{
					Name:  "label_3",
					Value: "value_3",
				}: codable.FingerprintSet{1: struct{}{}},
			},
		}, {
			fpToMetric: index.FingerprintMetricMapping{
				3: {
					clientmodel.MetricNameLabel: "metric_0",
					"label_1":                   "value_3",
				},
				4: {
					clientmodel.MetricNameLabel: "metric_2",
					"label_2":                   "value_2",
					"label_3":                   "value_1",
				},
				5: {
					clientmodel.MetricNameLabel: "metric_1",
					"label_1":                   "value_3",
				},
			},
			expectedLnToLvs: index.LabelNameLabelValuesMapping{
				clientmodel.MetricNameLabel: codable.LabelValueSet{
					"metric_0": struct{}{},
					"metric_1": struct{}{},
					"metric_2": struct{}{},
				},
				"label_1": codable.LabelValueSet{
					"value_1": struct{}{},
					"value_2": struct{}{},
					"value_3": struct{}{},
				},
				"label_2": codable.LabelValueSet{
					"value_2": struct{}{},
				},
				"label_3": codable.LabelValueSet{
					"value_1": struct{}{},
					"value_3": struct{}{},
				},
			},
			expectedLpToFps: index.LabelPairFingerprintsMapping{
				metric.LabelPair{
					Name:  clientmodel.MetricNameLabel,
					Value: "metric_0",
				}: codable.FingerprintSet{0: struct{}{}, 1: struct{}{}, 3: struct{}{}},
				metric.LabelPair{
					Name:  clientmodel.MetricNameLabel,
					Value: "metric_1",
				}: codable.FingerprintSet{2: struct{}{}, 5: struct{}{}},
				metric.LabelPair{
					Name:  clientmodel.MetricNameLabel,
					Value: "metric_2",
				}: codable.FingerprintSet{4: struct{}{}},
				metric.LabelPair{
					Name:  "label_1",
					Value: "value_1",
				}: codable.FingerprintSet{0: struct{}{}},
				metric.LabelPair{
					Name:  "label_1",
					Value: "value_2",
				}: codable.FingerprintSet{2: struct{}{}},
				metric.LabelPair{
					Name:  "label_1",
					Value: "value_3",
				}: codable.FingerprintSet{3: struct{}{}, 5: struct{}{}},
				metric.LabelPair{
					Name:  "label_2",
					Value: "value_2",
				}: codable.FingerprintSet{1: struct{}{}, 4: struct{}{}},
				metric.LabelPair{
					Name:  "label_3",
					Value: "value_1",
				}: codable.FingerprintSet{4: struct{}{}},
				metric.LabelPair{
					Name:  "label_3",
					Value: "value_3",
				}: codable.FingerprintSet{1: struct{}{}},
			},
		},
	}

	p, closer := newTestPersistence(t)
	defer closer.Close()

	indexedFpsToMetrics := index.FingerprintMetricMapping{}
	for i, b := range batches {
		for fp, m := range b.fpToMetric {
			p.indexMetric(m, fp)
			if err := p.archiveMetric(fp, m, 1, 2); err != nil {
				t.Fatal(err)
			}
			indexedFpsToMetrics[fp] = m
		}
		verifyIndexedState(i, t, b, indexedFpsToMetrics, p)
	}

	for i := len(batches) - 1; i >= 0; i-- {
		b := batches[i]
		verifyIndexedState(i, t, batches[i], indexedFpsToMetrics, p)
		for fp, m := range b.fpToMetric {
			p.unindexMetric(m, fp)
			unarchived, err := p.unarchiveMetric(fp)
			if err != nil {
				t.Fatal(err)
			}
			if !unarchived {
				t.Errorf("%d. metric not unarchived", i)
			}
			delete(indexedFpsToMetrics, fp)
		}
	}
}

func verifyIndexedState(i int, t *testing.T, b incrementalBatch, indexedFpsToMetrics index.FingerprintMetricMapping, p *persistence) {
	p.waitForIndexing()
	for fp, m := range indexedFpsToMetrics {
		// Compare archived metrics with input metrics.
		mOut, err := p.getArchivedMetric(fp)
		if err != nil {
			t.Fatal(err)
		}
		if !mOut.Equal(m) {
			t.Errorf("%d. %v: Got: %s; want %s", i, fp, mOut, m)
		}

		// Check that archived metrics are in membership index.
		has, first, last, err := p.hasArchivedMetric(fp)
		if err != nil {
			t.Fatal(err)
		}
		if !has {
			t.Errorf("%d. fingerprint %v not found", i, fp)
		}
		if first != 1 || last != 2 {
			t.Errorf(
				"%d. %v: Got first: %d, last %d; want first: %d, last %d",
				i, fp, first, last, 1, 2,
			)
		}
	}

	// Compare label name -> label values mappings.
	for ln, lvs := range b.expectedLnToLvs {
		outLvs, err := p.getLabelValuesForLabelName(ln)
		if err != nil {
			t.Fatal(err)
		}

		outSet := codable.LabelValueSet{}
		for _, lv := range outLvs {
			outSet[lv] = struct{}{}
		}

		if !reflect.DeepEqual(lvs, outSet) {
			t.Errorf("%d. label values don't match. Got: %v; want %v", i, outSet, lvs)
		}
	}

	// Compare label pair -> fingerprints mappings.
	for lp, fps := range b.expectedLpToFps {
		outFps, err := p.getFingerprintsForLabelPair(lp)
		if err != nil {
			t.Fatal(err)
		}

		outSet := codable.FingerprintSet{}
		for _, fp := range outFps {
			outSet[fp] = struct{}{}
		}

		if !reflect.DeepEqual(fps, outSet) {
			t.Errorf("%d. %v: fingerprints don't match. Got: %v; want %v", i, lp, outSet, fps)
		}
	}
}
