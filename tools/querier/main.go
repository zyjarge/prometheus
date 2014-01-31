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

	"github.com/prometheus/prometheus/rules"
	"github.com/prometheus/prometheus/rules/ast"
	"github.com/prometheus/prometheus/stats"
	"github.com/prometheus/prometheus/storage/metric"
)

// Commandline flags.
var (
	storageRoot      = flag.String("storage.root", "/tmp/metrics", "Base path for metrics storage.")
	expression       = flag.String("expression", "up", "Expression language query to execute.")
	numWarmupQueries = flag.Int("numWarmupQueries", 10, "How many times the provided query should be executed before measuring time.")
	numQueries       = flag.Int("numQueries", 100, "How many times the provided query should be repeated when measuring time.")
	queryRange       = flag.Duration("range", time.Hour, "How long of a time range to run the query over.")
	queryEnd         = flag.Int("end", int(time.Date(2014, 1, 1, 0, 0, 0, 0, time.UTC).Unix()), "The end of the query range, as a number of seconds since the epoch.")
	queryResolution  = flag.Duration("resolution", time.Minute, "What query resolution to use.")
	expectedElements = flag.Int("expectedElements", -1, "How many vector elements to expect in the result. -1 indicates no expectation will be checked.")
)

func main() {
	flag.Parse()

	ts, err := metric.NewTieredStorage(1, 1, time.Hour, time.Hour, *storageRoot)
	if err != nil {
		glog.Fatal("Error opening storage: ", err)
	}
	defer ts.Close()

	storageStarted := make(chan bool)
	go ts.Serve(storageStarted)
	<-storageStarted

	expr, err := rules.LoadExprFromString(*expression)
	if expr.Type() != ast.VECTOR {
		glog.Fatal("Expression does not evaluate to vector type")
	}

	vectorExpr := expr.(ast.VectorNode)
	start := clientmodel.TimestampFromUnix(int64(*queryEnd - int(*queryRange/time.Second)))
	end := clientmodel.TimestampFromUnix(int64(*queryEnd))

	doQuery := func(s *stats.TimerGroup, warmup bool) {
		matrix, err := ast.EvalVectorRange(vectorExpr, start, end, *queryResolution, ts, s)
		if err != nil {
			glog.Fatal(err)
		}
		if warmup {
			if *expectedElements >= 0 && len(matrix) != *expectedElements {
				glog.Fatalf("Expected %d elements, got %d", *expectedElements, len(matrix))
			}
			for _, el := range matrix {
				expectedValues := int(*queryRange / *queryResolution)
				if len(el.Values) != expectedValues {
					glog.Fatalf("Expected %d values, got %d", expectedValues, len(el.Values))
				}
			}
		}
	}

	queryStats := stats.NewTimerGroup()
	for i := 0; i < *numWarmupQueries; i++ {
		doQuery(queryStats, true)
	}

	queryStats = stats.NewTimerGroup()
	for i := 0; i < *numQueries; i++ {
		doQuery(queryStats, false)
	}

	glog.Infof("Average query stats:\n%s\n%s", expr, queryStats.AvgString(*numQueries))
}
