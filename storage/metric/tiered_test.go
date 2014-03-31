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

package metric

import (
	"math"
	"reflect"
	"sort"
	"testing"
	"time"

	clientmodel "github.com/prometheus/client_golang/model"

	"github.com/prometheus/prometheus/stats"
	"github.com/prometheus/prometheus/storage/remote"
	"github.com/prometheus/prometheus/utility/test"
)

type mockTSDB struct {
	i                   int
	Count               int
	t                   test.Tester
	expectedMetric      clientmodel.Metric
	expectedStartTime   clientmodel.Timestamp
	expectedEndTime     clientmodel.Timestamp
	cannedResultSamples clientmodel.Samples
	cannedResultError   error
}

func newMockTSDB(t test.Tester, i int) *mockTSDB {
	return &mockTSDB{i: i, t: t}
}

func (tsdb *mockTSDB) Store(clientmodel.Samples) error {
	// Deliberately not implemented.
	panic("unexpected call of Store")
}

func (tsdb *mockTSDB) Retrieve(
	metric clientmodel.Metric,
	startTime clientmodel.Timestamp,
	endTime clientmodel.Timestamp,
) (clientmodel.Samples, error) {
	tsdb.Count++
	if !reflect.DeepEqual(metric, tsdb.expectedMetric) ||
		startTime != tsdb.expectedStartTime ||
		endTime != tsdb.expectedEndTime {
		tsdb.t.Errorf(
			"%d. TSDBClient.Retrieve(%#v, %v, %v), want Retrieve(%#v, %v, %v)",
			tsdb.i,
			metric, startTime, endTime,
			tsdb.expectedMetric, tsdb.expectedStartTime, tsdb.expectedEndTime,
		)
	}
	return tsdb.cannedResultSamples, tsdb.cannedResultError
}

func (tsdb *mockTSDB) SetExpectedRetrieve(
	metric clientmodel.Metric,
	startTime clientmodel.Timestamp,
	endTime clientmodel.Timestamp,
) {
	tsdb.expectedMetric = metric
	tsdb.expectedStartTime = startTime
	tsdb.expectedEndTime = endTime
}

func (tsdb *mockTSDB) SetResult(samples clientmodel.Samples, err error) {
	tsdb.cannedResultSamples = samples
	tsdb.cannedResultError = err
}

func buildSamples(from, to clientmodel.Timestamp, interval time.Duration, m clientmodel.Metric) (v clientmodel.Samples) {
	i := clientmodel.SampleValue(0)

	for from.Before(to) {
		v = append(v, &clientmodel.Sample{
			Metric:    m,
			Value:     i,
			Timestamp: from,
		})

		from = from.Add(interval)
		i++
	}

	return
}

func buildValues(firstValue clientmodel.SampleValue, from, to clientmodel.Timestamp, interval time.Duration) (v Values) {
	for from.Before(to) {
		v = append(v, SamplePair{
			Value:     firstValue,
			Timestamp: from,
		})

		from = from.Add(interval)
		firstValue++
	}

	return
}

func testMakeView(t test.Tester, flushToDisk bool, useTSDB bool) {
	type in struct {
		atTime     []getValuesAtTimeOp
		atInterval []getValuesAtIntervalOp
		alongRange []getValuesAlongRangeOp
	}

	type out struct {
		atTime     []Values
		atInterval []Values
		alongRange []Values
	}
	type tsdbCall struct {
		startTime clientmodel.Timestamp // 0 => 'no calls expected'.
		endTime   clientmodel.Timestamp
		samples   clientmodel.Samples
	}
	metric := clientmodel.Metric{clientmodel.MetricNameLabel: "request_count"}
	fingerprint := &clientmodel.Fingerprint{}
	fingerprint.LoadFromMetric(metric)
	var (
		instant   = clientmodel.TimestampFromTime(time.Date(1984, 3, 30, 0, 0, 0, 0, time.Local))
		scenarios = []struct {
			data     clientmodel.Samples
			in       in
			out      out
			tsdbCall tsdbCall
			diskOnly bool
			tsdbOnly bool
		}{
			// 0. No sample, but query asks for one.
			{
				in: in{
					atTime: []getValuesAtTimeOp{
						{
							baseOp: baseOp{current: instant},
						},
					},
				},
				out: out{
					atTime: []Values{{}},
				},
			},
			// 1. Single sample, query asks for exact sample time.
			{
				data: clientmodel.Samples{
					{
						Metric:    metric,
						Value:     0,
						Timestamp: instant,
					},
				},
				in: in{
					atTime: []getValuesAtTimeOp{
						{
							baseOp: baseOp{current: instant},
						},
					},
				},
				out: out{
					atTime: []Values{
						{
							{
								Timestamp: instant,
								Value:     0,
							},
						},
					},
				},
			},
			// 2. Single sample, query time before the sample.
			{
				data: clientmodel.Samples{
					{
						Metric:    metric,
						Value:     0,
						Timestamp: instant.Add(time.Second),
					},
					{
						Metric:    metric,
						Value:     1,
						Timestamp: instant.Add(time.Second * 2),
					},
				},
				in: in{
					atTime: []getValuesAtTimeOp{
						{
							baseOp: baseOp{current: instant},
						},
					},
				},
				out: out{
					atTime: []Values{
						{
							{
								Timestamp: instant.Add(time.Second),
								Value:     0,
							},
						},
					},
				},
				tsdbCall: tsdbCall{
					startTime: instant,
					endTime:   instant.Add(time.Second),
					samples:   clientmodel.Samples{},
				},
			},
			// 3. Single sample, query time after the sample.
			{
				data: clientmodel.Samples{
					{
						Metric:    metric,
						Value:     0,
						Timestamp: instant,
					},
				},
				in: in{
					atTime: []getValuesAtTimeOp{
						{
							baseOp: baseOp{current: instant.Add(time.Second)},
						},
					},
				},
				out: out{
					atTime: []Values{
						{
							{
								Timestamp: instant,
								Value:     0,
							},
						},
					},
				},
			},
			// 4. Two samples, query asks for first sample time.
			{
				data: clientmodel.Samples{
					{
						Metric:    metric,
						Value:     0,
						Timestamp: instant,
					},
					{
						Metric:    metric,
						Value:     1,
						Timestamp: instant.Add(time.Second),
					},
				},
				in: in{
					atTime: []getValuesAtTimeOp{
						{
							baseOp: baseOp{current: instant},
						},
					},
				},
				out: out{
					atTime: []Values{
						{
							{
								Timestamp: instant,
								Value:     0,
							},
						},
					},
				},
			},
			// 5. Three samples, query asks for second sample time.
			{
				data: clientmodel.Samples{
					{
						Metric:    metric,
						Value:     0,
						Timestamp: instant,
					},
					{
						Metric:    metric,
						Value:     1,
						Timestamp: instant.Add(time.Second),
					},
					{
						Metric:    metric,
						Value:     2,
						Timestamp: instant.Add(time.Second * 2),
					},
				},
				in: in{
					atTime: []getValuesAtTimeOp{
						{
							baseOp: baseOp{current: instant.Add(time.Second)},
						},
					},
				},
				out: out{
					atTime: []Values{
						{
							{
								Timestamp: instant.Add(time.Second),
								Value:     1,
							},
						},
					},
				},
			},
			// 6. Three samples, query asks for time between first and second samples.
			{
				data: clientmodel.Samples{
					{
						Metric:    metric,
						Value:     0,
						Timestamp: instant,
					},
					{
						Metric:    metric,
						Value:     1,
						Timestamp: instant.Add(time.Second * 2),
					},
					{
						Metric:    metric,
						Value:     2,
						Timestamp: instant.Add(time.Second * 4),
					},
				},
				in: in{
					atTime: []getValuesAtTimeOp{
						{
							baseOp: baseOp{current: instant.Add(time.Second)},
						},
					},
				},
				out: out{
					atTime: []Values{
						{
							{
								Timestamp: instant,
								Value:     0,
							},
							{
								Timestamp: instant.Add(time.Second * 2),
								Value:     1,
							},
						},
					},
				},
			},
			// 7. Three samples, query asks for time between second and third samples.
			{
				data: clientmodel.Samples{
					{
						Metric:    metric,
						Value:     0,
						Timestamp: instant,
					},
					{
						Metric:    metric,
						Value:     1,
						Timestamp: instant.Add(time.Second * 2),
					},
					{
						Metric:    metric,
						Value:     2,
						Timestamp: instant.Add(time.Second * 4),
					},
				},
				in: in{
					atTime: []getValuesAtTimeOp{
						{
							baseOp: baseOp{current: instant.Add(time.Second * 3)},
						},
					},
				},
				out: out{
					atTime: []Values{
						{
							{
								Timestamp: instant.Add(time.Second * 2),
								Value:     1,
							},
							{
								Timestamp: instant.Add(time.Second * 4),
								Value:     2,
							},
						},
					},
				},
			},
			// 8. Two chunks of samples, query asks for values from second chunk.
			{
				data: buildSamples(
					instant,
					instant.Add(time.Duration(*leveldbChunkSize*4)*time.Second),
					2*time.Second,
					metric,
				),
				in: in{
					atTime: []getValuesAtTimeOp{
						{
							baseOp: baseOp{current: instant.Add(time.Second*time.Duration(*leveldbChunkSize*2) + clientmodel.MinimumTick)},
						},
					},
				},
				out: out{
					atTime: []Values{
						{
							{
								Timestamp: instant.Add(time.Second * time.Duration(*leveldbChunkSize*2)),
								Value:     200,
							},
							{
								Timestamp: instant.Add(time.Second * (time.Duration(*leveldbChunkSize*2) + 2)),
								Value:     201,
							},
						},
					},
				},
			},
			// 9. Two chunks of samples, query asks for values between both chunks.
			{
				data: buildSamples(
					instant,
					instant.Add(time.Duration(*leveldbChunkSize*4)*time.Second),
					2*time.Second,
					metric,
				),
				in: in{
					atTime: []getValuesAtTimeOp{
						{
							baseOp: baseOp{current: instant.Add(time.Second*time.Duration(*leveldbChunkSize*2) - clientmodel.MinimumTick)},
						},
					},
				},
				out: out{
					atTime: []Values{
						{
							{
								Timestamp: instant.Add(time.Second * (time.Duration(*leveldbChunkSize*2) - 2)),
								Value:     199,
							},
							{
								Timestamp: instant.Add(time.Second * time.Duration(*leveldbChunkSize*2)),
								Value:     200,
							},
						},
					},
				},
			},
			// 10. Two chunks of samples, getValuesAtIntervalOp spanning both.
			{
				data: buildSamples(
					instant,
					instant.Add(time.Duration(*leveldbChunkSize*6)*time.Second),
					2*time.Second,
					metric,
				),
				in: in{
					atInterval: []getValuesAtIntervalOp{
						{
							getValuesAlongRangeOp: getValuesAlongRangeOp{
								baseOp:  baseOp{current: instant.Add(time.Second*time.Duration(*leveldbChunkSize*2-4) - clientmodel.MinimumTick)},
								through: instant.Add(time.Second*time.Duration(*leveldbChunkSize*2+4) + clientmodel.MinimumTick),
							},
							interval: time.Second * 6,
						},
					},
				},
				out: out{
					atInterval: []Values{
						{
							{
								Timestamp: instant.Add(time.Second * time.Duration(*leveldbChunkSize*2-6)),
								Value:     197,
							},
							{
								Timestamp: instant.Add(time.Second * time.Duration(*leveldbChunkSize*2-4)),
								Value:     198,
							},
							{
								Timestamp: instant.Add(time.Second * time.Duration(*leveldbChunkSize*2)),
								Value:     200,
							},
							{
								Timestamp: instant.Add(time.Second * time.Duration(*leveldbChunkSize*2+2)),
								Value:     201,
							},
						},
					},
				},
			},
			// 11. Three chunks of samples, getValuesAlongRangeOp spanning all of them.
			{
				data: buildSamples(
					instant,
					instant.Add(time.Duration(*leveldbChunkSize*6)*time.Second),
					2*time.Second,
					metric,
				),
				in: in{
					alongRange: []getValuesAlongRangeOp{
						{
							baseOp:  baseOp{current: instant.Add(time.Second*time.Duration(*leveldbChunkSize*2-4) - clientmodel.MinimumTick)},
							through: instant.Add(time.Second*time.Duration(*leveldbChunkSize*4+2) + clientmodel.MinimumTick),
						},
					},
				},
				out: out{
					alongRange: []Values{buildValues(
						clientmodel.SampleValue(198),
						instant.Add(time.Second*time.Duration(*leveldbChunkSize*2-4)),
						instant.Add(time.Second*time.Duration(*leveldbChunkSize*4+2)+clientmodel.MinimumTick),
						2*time.Second,
					)},
				},
			},
			// 12. Three chunks of samples and a getValuesAlongIntervalOp with an
			// interval larger than the natural sample interval, spanning the gap
			// between the second and third chunks. To test two consecutive
			// ExtractSamples() calls for the same op, we need three on-disk chunks,
			// because the first two chunks are loaded from disk together and passed
			// as one unit into ExtractSamples(). Especially, we want to test that
			// the first sample of the last chunk is included in the result.
			//
			// This is a regression test for an interval operator advancing too far
			// past the end of the currently available chunk, effectively skipping
			// over a value which is only available in the next chunk passed to
			// ExtractSamples().
			//
			// Chunk and operator layout, assuming 200 samples per chunk:
			//
			//         Chunk 1      Chunk 2        Chunk 3
			// Values: 0......199   200......399   400......599
			// Times:  0......398   400......798   800......1198
			//              |                          |
			//              |_________ Operator _______|
			//             395 399 ......  795  799  803
			{
				data: buildSamples(
					instant,
					instant.Add(time.Duration(*leveldbChunkSize*6)*time.Second),
					2*time.Second,
					metric,
				),
				in: in{
					atInterval: []getValuesAtIntervalOp{
						{
							getValuesAlongRangeOp: getValuesAlongRangeOp{
								baseOp:  baseOp{current: instant.Add(time.Second * time.Duration(*leveldbChunkSize*2-5))},
								through: instant.Add(time.Second * time.Duration(*leveldbChunkSize*4+3)),
							},
							interval: time.Second * 4,
						},
					},
				},
				out: out{
					atInterval: []Values{
						// We need two overlapping buildValues() calls here since the last
						// value of the second chunk is extracted twice (value 399, time
						// offset 798s).
						append(
							// Values 197...399.
							// Times  394...798.
							buildValues(
								clientmodel.SampleValue(197),
								instant.Add(time.Second*time.Duration(*leveldbChunkSize*2-6)),
								instant.Add(time.Second*time.Duration(*leveldbChunkSize*4)),
								2*time.Second,
							),
							// Values 399...402.
							// Times  798...804.
							buildValues(
								clientmodel.SampleValue(399),
								instant.Add(time.Second*time.Duration(*leveldbChunkSize*4-2)),
								instant.Add(time.Second*time.Duration(*leveldbChunkSize*4+6)),
								2*time.Second,
							)...,
						),
					},
				},
				// This example only works with on-disk chunks due to the repeatedly
				// extracted value at the end of the second chunk.
				diskOnly: true,
			},
			// 13. Single sample, getValuesAtIntervalOp starting after the sample.
			{
				data: clientmodel.Samples{
					{
						Metric:    metric,
						Value:     0,
						Timestamp: instant,
					},
				},
				in: in{
					atInterval: []getValuesAtIntervalOp{
						{
							getValuesAlongRangeOp: getValuesAlongRangeOp{
								baseOp:  baseOp{current: instant.Add(time.Second)},
								through: instant.Add(time.Second * 2),
							},
							interval: time.Second,
						},
					},
				},
				out: out{
					atInterval: []Values{
						{
							{
								Timestamp: instant,
								Value:     0,
							},
						},
					},
				},
			},
			// 14. Single sample, getValuesAtIntervalOp starting before the sample.
			{
				data: clientmodel.Samples{
					{
						Metric:    metric,
						Value:     0,
						Timestamp: instant.Add(time.Second),
					},
				},
				in: in{
					atInterval: []getValuesAtIntervalOp{
						{
							getValuesAlongRangeOp: getValuesAlongRangeOp{
								baseOp:  baseOp{current: instant},
								through: instant.Add(time.Second * 2),
							},
							interval: time.Second,
						},
					},
				},
				out: out{
					atInterval: []Values{
						{
							{
								Timestamp: instant.Add(time.Second),
								Value:     0,
							},
							{
								Timestamp: instant.Add(time.Second),
								Value:     0,
							},
						},
					},
				},
				tsdbCall: tsdbCall{
					startTime: instant,
					endTime:   instant.Add(time.Second),
					samples:   clientmodel.Samples{},
				},
			},
			// 15. Single sample, query time before the sample, similar to 2. but now TSDB has a value before the query time.
			{
				data: clientmodel.Samples{
					{
						Metric:    metric,
						Value:     0,
						Timestamp: instant.Add(time.Second),
					},
					{
						Metric:    metric,
						Value:     1,
						Timestamp: instant.Add(time.Second * 2),
					},
				},
				in: in{
					atTime: []getValuesAtTimeOp{
						{
							baseOp: baseOp{current: instant},
						},
					},
				},
				out: out{
					atTime: []Values{
						{
							{
								Timestamp: instant.Add(-time.Second),
								Value:     -2,
							},
							{
								Timestamp: instant.Add(time.Second),
								Value:     0,
							},
						},
					},
				},
				tsdbCall: tsdbCall{
					startTime: instant,
					endTime:   instant.Add(time.Second),
					samples: clientmodel.Samples{
						&clientmodel.Sample{Metric: metric, Value: -2, Timestamp: instant.Add(-time.Second)},
					},
				},
				tsdbOnly: true,
			},
			// 16. Single sample, query time before the sample, similar to 2. but now TSDB has a value at exactly the query time.
			{
				data: clientmodel.Samples{
					{
						Metric:    metric,
						Value:     0,
						Timestamp: instant.Add(time.Second),
					},
					{
						Metric:    metric,
						Value:     1,
						Timestamp: instant.Add(time.Second * 2),
					},
				},
				in: in{
					atTime: []getValuesAtTimeOp{
						{
							baseOp: baseOp{current: instant},
						},
					},
				},
				out: out{
					atTime: []Values{
						{
							{
								Timestamp: instant,
								Value:     -1,
							},
						},
					},
				},
				tsdbCall: tsdbCall{
					startTime: instant,
					endTime:   instant.Add(time.Second),
					samples: clientmodel.Samples{
						&clientmodel.Sample{Metric: metric, Value: -1, Timestamp: instant},
					},
				},
				tsdbOnly: true,
			},
			// 17. Single sample, query time before the sample, similar to 2. but now TSDB has a value after the query time.
			// Note that TSDB will _not_ return a value before the query time (in contrast to the local storage).
			{
				data: clientmodel.Samples{
					{
						Metric:    metric,
						Value:     0,
						Timestamp: instant.Add(time.Second),
					},
					{
						Metric:    metric,
						Value:     1,
						Timestamp: instant.Add(time.Second * 2),
					},
				},
				in: in{
					atTime: []getValuesAtTimeOp{
						{
							baseOp: baseOp{current: instant.Add(-time.Second)},
						},
					},
				},
				out: out{
					atTime: []Values{
						{
							{
								Timestamp: instant,
								Value:     -1,
							},
						},
					},
				},
				tsdbCall: tsdbCall{
					startTime: instant.Add(-time.Second),
					endTime:   instant.Add(time.Second),
					samples: clientmodel.Samples{
						&clientmodel.Sample{Metric: metric, Value: -1, Timestamp: instant},
					},
				},
				tsdbOnly: true,
			},
			// 18. getValuesAtIntervalOp starting before the sample, with values in TSDB.
			{
				data: clientmodel.Samples{
					{
						Metric:    metric,
						Value:     0,
						Timestamp: instant.Add(time.Second),
					},
				},
				in: in{
					atInterval: []getValuesAtIntervalOp{
						{
							getValuesAlongRangeOp: getValuesAlongRangeOp{
								baseOp:  baseOp{current: instant.Add(-time.Second)},
								through: instant.Add(time.Second * 2),
							},
							interval: time.Second,
						},
					},
				},
				out: out{
					atInterval: []Values{
						{
							{
								Timestamp: instant.Add(-time.Second),
								Value:     -2,
							},
							{
								Timestamp: instant,
								Value:     -1,
							},
							{
								Timestamp: instant.Add(time.Second),
								Value:     0,
							},
							{
								Timestamp: instant.Add(time.Second),
								Value:     0,
							},
						},
					},
				},
				tsdbCall: tsdbCall{
					startTime: instant.Add(-time.Second),
					endTime:   instant.Add(time.Second),
					samples: clientmodel.Samples{
						&clientmodel.Sample{Metric: metric, Value: -2, Timestamp: instant.Add(-time.Second)},
						&clientmodel.Sample{Metric: metric, Value: -1, Timestamp: instant},
					},
				},
				tsdbOnly: true,
			},
		}
	)

	for i, scenario := range scenarios {
		if scenario.diskOnly && !flushToDisk {
			continue
		}
		if scenario.tsdbOnly && !useTSDB {
			continue
		}

		var tsdb *mockTSDB
		var tsdbClient remote.TSDBClient
		if useTSDB {
			tsdb = newMockTSDB(t, i)
			if scenario.tsdbCall.startTime != 0 {
				tsdb.SetExpectedRetrieve(metric, scenario.tsdbCall.startTime, scenario.tsdbCall.endTime)
				tsdb.SetResult(scenario.tsdbCall.samples, nil)
			}
			tsdbClient = tsdb
		}
		tiered, closer := NewTestTieredStorage(t, tsdbClient)

		err := tiered.AppendSamples(scenario.data)
		if err != nil {
			t.Fatalf("%d. failed to add fixture data: %s", i, err)
		}

		if flushToDisk {
			tiered.Flush()
		}

		requestBuilder := NewViewRequestBuilder()

		for _, atTime := range scenario.in.atTime {
			requestBuilder.GetMetricAtTime(fingerprint, atTime.current)
		}

		for _, atInterval := range scenario.in.atInterval {
			requestBuilder.GetMetricAtInterval(fingerprint, atInterval.current, atInterval.through, atInterval.interval)
		}

		for _, alongRange := range scenario.in.alongRange {
			requestBuilder.GetMetricRange(fingerprint, alongRange.current, alongRange.through)
		}

		v, err := tiered.MakeView(requestBuilder, time.Second*5, stats.NewTimerGroup())

		if err != nil {
			t.Fatalf("%d. failed due to %s", i, err)
		}

		// To get all values in the View, ask for the 'forever' interval.
		interval := Interval{OldestInclusive: math.MinInt64, NewestInclusive: math.MaxInt64}

		for j, atTime := range scenario.out.atTime {
			actual := v.GetRangeValues(fingerprint, interval)

			if len(actual) != len(atTime) {
				t.Fatalf("%d.%d. expected %d output, got %d", i, j, len(atTime), len(actual))
			}

			for k, value := range atTime {
				if value.Value != actual[k].Value {
					t.Errorf("%d.%d.%d expected %v value, got %v", i, j, k, value.Value, actual[k].Value)
				}
				if !value.Timestamp.Equal(actual[k].Timestamp) {
					t.Errorf("%d.%d.%d expected %s (offset %ss) timestamp, got %s (offset %ss)", i, j, k, value.Timestamp, value.Timestamp.Sub(instant), actual[k].Timestamp, actual[k].Timestamp.Sub(instant))
				}
			}
		}

		for j, atInterval := range scenario.out.atInterval {
			actual := v.GetRangeValues(fingerprint, interval)

			if len(actual) != len(atInterval) {
				t.Fatalf("%d.%d. expected %d output, got %d", i, j, len(atInterval), len(actual))
			}

			for k, value := range atInterval {
				if value.Value != actual[k].Value {
					t.Errorf("%d.%d.%d expected %v value, got %v", i, j, k, value.Value, actual[k].Value)
				}
				if !value.Timestamp.Equal(actual[k].Timestamp) {
					t.Errorf("%d.%d.%d expected %s (offset %ds) timestamp, got %s (offset %ds, value %s)", i, j, k, value.Timestamp, int(value.Timestamp.Sub(instant)/time.Second), actual[k].Timestamp, int(actual[k].Timestamp.Sub(instant)/time.Second), actual[k].Value)
				}
			}
		}

		for j, alongRange := range scenario.out.alongRange {
			actual := v.GetRangeValues(fingerprint, interval)

			if len(actual) != len(alongRange) {
				t.Fatalf("%d.%d. expected %d output, got %d", i, j, len(alongRange), len(actual))
			}

			for k, value := range alongRange {
				if value.Value != actual[k].Value {
					t.Errorf("%d.%d.%d expected %v value, got %v", i, j, k, value.Value, actual[k].Value)
				}
				if !value.Timestamp.Equal(actual[k].Timestamp) {
					t.Errorf("%d.%d.%d expected %s (offset %ss) timestamp, got %s (offset %ss)", i, j, k, value.Timestamp, value.Timestamp.Sub(instant), actual[k].Timestamp, actual[k].Timestamp.Sub(instant))
				}
			}
		}

		if useTSDB {
			if scenario.tsdbCall.startTime == 0 && tsdb.Count > 0 {
				t.Errorf("%d. expected no TSDBClient.Retrieve call, got %d", i, tsdb.Count)
			} else if scenario.tsdbCall.startTime != 0 && tsdb.Count != 1 {
				t.Errorf("%d. expected exactly 1 TSDBClient.Retrieve call, got %d", i, tsdb.Count)
			}
		}
		closer.Close()
	}
}

func TestMakeViewFlush(t *testing.T) {
	testMakeView(t, true, false)
}

func BenchmarkMakeViewFlush(b *testing.B) {
	for i := 0; i < b.N; i++ {
		testMakeView(b, true, false)
	}
}

func TestMakeViewNoFlush(t *testing.T) {
	testMakeView(t, false, false)
}

func BenchmarkMakeViewNoFlush(b *testing.B) {
	for i := 0; i < b.N; i++ {
		testMakeView(b, false, false)
	}
}

func TestMakeViewTSDB(t *testing.T) {
	testMakeView(t, true, true)
}

func TestGetAllValuesForLabel(t *testing.T) {
	type in struct {
		metricName     string
		appendToMemory bool
		appendToDisk   bool
	}

	scenarios := []struct {
		in  []in
		out []string
	}{
		{
		// Empty case.
		}, {
			in: []in{
				{
					metricName:     "request_count",
					appendToMemory: false,
					appendToDisk:   true,
				},
			},
			out: []string{
				"request_count",
			},
		}, {
			in: []in{
				{
					metricName:     "request_count",
					appendToMemory: true,
					appendToDisk:   false,
				},
				{
					metricName:     "start_time",
					appendToMemory: false,
					appendToDisk:   true,
				},
			},
			out: []string{
				"request_count",
				"start_time",
			},
		}, {
			in: []in{
				{
					metricName:     "request_count",
					appendToMemory: true,
					appendToDisk:   true,
				},
				{
					metricName:     "start_time",
					appendToMemory: true,
					appendToDisk:   true,
				},
			},
			out: []string{
				"request_count",
				"start_time",
			},
		},
	}

	for i, scenario := range scenarios {
		tiered, closer := NewTestTieredStorage(t, nil)
		for j, metric := range scenario.in {
			sample := &clientmodel.Sample{
				Metric: clientmodel.Metric{clientmodel.MetricNameLabel: clientmodel.LabelValue(metric.metricName)},
			}
			if metric.appendToMemory {
				if err := tiered.memoryArena.AppendSample(sample); err != nil {
					t.Fatalf("%d.%d. failed to add fixture data: %s", i, j, err)
				}
			}
			if metric.appendToDisk {
				if err := tiered.DiskStorage.AppendSample(sample); err != nil {
					t.Fatalf("%d.%d. failed to add fixture data: %s", i, j, err)
				}
			}
		}
		metricNames, err := tiered.GetAllValuesForLabel(clientmodel.MetricNameLabel)
		closer.Close()
		if err != nil {
			t.Fatalf("%d. Error getting metric names: %s", i, err)
		}
		if len(metricNames) != len(scenario.out) {
			t.Fatalf("%d. Expected metric count %d, got %d", i, len(scenario.out), len(metricNames))
		}

		sort.Sort(metricNames)
		for j, expected := range scenario.out {
			if expected != string(metricNames[j]) {
				t.Fatalf("%d.%d. Expected metric %s, got %s", i, j, expected, metricNames[j])
			}
		}
	}
}

func TestGetFingerprintsForLabelSet(t *testing.T) {
	tiered, closer := NewTestTieredStorage(t, nil)
	defer closer.Close()
	memorySample := &clientmodel.Sample{
		Metric: clientmodel.Metric{clientmodel.MetricNameLabel: "http_requests", "method": "/foo"},
	}
	diskSample := &clientmodel.Sample{
		Metric: clientmodel.Metric{clientmodel.MetricNameLabel: "http_requests", "method": "/bar"},
	}
	if err := tiered.memoryArena.AppendSample(memorySample); err != nil {
		t.Fatalf("Failed to add fixture data: %s", err)
	}
	if err := tiered.DiskStorage.AppendSample(diskSample); err != nil {
		t.Fatalf("Failed to add fixture data: %s", err)
	}
	tiered.Flush()

	scenarios := []struct {
		labels  clientmodel.LabelSet
		fpCount int
	}{
		{
			labels:  clientmodel.LabelSet{},
			fpCount: 0,
		}, {
			labels: clientmodel.LabelSet{
				clientmodel.MetricNameLabel: "http_requests",
			},
			fpCount: 2,
		}, {
			labels: clientmodel.LabelSet{
				clientmodel.MetricNameLabel: "http_requests",
				"method":                    "/foo",
			},
			fpCount: 1,
		}, {
			labels: clientmodel.LabelSet{
				clientmodel.MetricNameLabel: "http_requests",
				"method":                    "/bar",
			},
			fpCount: 1,
		}, {
			labels: clientmodel.LabelSet{
				clientmodel.MetricNameLabel: "http_requests",
				"method":                    "/baz",
			},
			fpCount: 0,
		},
	}

	for i, scenario := range scenarios {
		fingerprints, err := tiered.GetFingerprintsForLabelSet(scenario.labels)
		if err != nil {
			t.Fatalf("%d. Error getting metric names: %s", i, err)
		}
		if len(fingerprints) != scenario.fpCount {
			t.Fatalf("%d. Expected metric count %d, got %d", i, scenario.fpCount, len(fingerprints))
		}
	}
}

func TestTruncateBefore(t *testing.T) {
	type in struct {
		values Values
		time   clientmodel.Timestamp
	}
	instant := clientmodel.Now()
	var scenarios = []struct {
		in  in
		out Values
	}{
		{
			in: in{
				time: instant,
				values: Values{
					{
						Value:     0,
						Timestamp: instant,
					},
					{
						Value:     1,
						Timestamp: instant.Add(time.Second),
					},
					{
						Value:     2,
						Timestamp: instant.Add(2 * time.Second),
					},
					{
						Value:     3,
						Timestamp: instant.Add(3 * time.Second),
					},
					{
						Value:     4,
						Timestamp: instant.Add(4 * time.Second),
					},
				},
			},
			out: Values{
				{
					Value:     0,
					Timestamp: instant,
				},
				{
					Value:     1,
					Timestamp: instant.Add(time.Second),
				},
				{
					Value:     2,
					Timestamp: instant.Add(2 * time.Second),
				},
				{
					Value:     3,
					Timestamp: instant.Add(3 * time.Second),
				},
				{
					Value:     4,
					Timestamp: instant.Add(4 * time.Second),
				},
			},
		},
		{
			in: in{
				time: instant.Add(2 * time.Second),
				values: Values{
					{
						Value:     0,
						Timestamp: instant,
					},
					{
						Value:     1,
						Timestamp: instant.Add(time.Second),
					},
					{
						Value:     2,
						Timestamp: instant.Add(2 * time.Second),
					},
					{
						Value:     3,
						Timestamp: instant.Add(3 * time.Second),
					},
					{
						Value:     4,
						Timestamp: instant.Add(4 * time.Second),
					},
				},
			},
			out: Values{
				{
					Value:     1,
					Timestamp: instant.Add(time.Second),
				},
				{
					Value:     2,
					Timestamp: instant.Add(2 * time.Second),
				},
				{
					Value:     3,
					Timestamp: instant.Add(3 * time.Second),
				},
				{
					Value:     4,
					Timestamp: instant.Add(4 * time.Second),
				},
			},
		},
		{
			in: in{
				time: instant.Add(5 * time.Second),
				values: Values{
					{
						Value:     0,
						Timestamp: instant,
					},
					{
						Value:     1,
						Timestamp: instant.Add(time.Second),
					},
					{
						Value:     2,
						Timestamp: instant.Add(2 * time.Second),
					},
					{
						Value:     3,
						Timestamp: instant.Add(3 * time.Second),
					},
					{
						Value:     4,
						Timestamp: instant.Add(4 * time.Second),
					},
				},
			},
			out: Values{
				// Preserve the last value in case it needs to be used for the next set.
				{
					Value:     4,
					Timestamp: instant.Add(4 * time.Second),
				},
			},
		},
	}

	for i, scenario := range scenarios {
		actual := chunk(scenario.in.values).TruncateBefore(scenario.in.time)

		if len(actual) != len(scenario.out) {
			t.Fatalf("%d. expected length of %d, got %d", i, len(scenario.out), len(actual))
		}

		for j, actualValue := range actual {
			if !actualValue.Equal(&scenario.out[j]) {
				t.Fatalf("%d.%d. expected %s, got %s", i, j, scenario.out[j], actualValue)
			}
		}
	}
}

func TestGetMetricForFingerprintCachesCopyOfMetric(t *testing.T) {
	ts, closer := NewTestTieredStorage(t, nil)
	defer closer.Close()

	m := clientmodel.Metric{
		clientmodel.MetricNameLabel: "testmetric",
	}
	samples := clientmodel.Samples{
		&clientmodel.Sample{
			Metric:    m,
			Value:     0,
			Timestamp: clientmodel.Now(),
		},
	}

	if err := ts.AppendSamples(samples); err != nil {
		t.Fatal(err)
	}

	ts.Flush()

	fp := &clientmodel.Fingerprint{}
	fp.LoadFromMetric(m)
	m, err := ts.GetMetricForFingerprint(fp)
	if err != nil {
		t.Fatal(err)
	}

	m[clientmodel.MetricNameLabel] = "changedmetric"

	m, err = ts.GetMetricForFingerprint(fp)
	if err != nil {
		t.Fatal(err)
	}
	if m[clientmodel.MetricNameLabel] != "testmetric" {
		t.Fatal("Metric name label value has changed: ", m[clientmodel.MetricNameLabel])
	}
}
