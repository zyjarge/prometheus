package tsdb

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"regexp"
	"time"

	"github.com/golang/glog"

	clientmodel "github.com/prometheus/client_golang/model"

	"github.com/prometheus/prometheus/utility"
)

const (
	putEndpoint     = "/api/put"
	contentTypeJson = "application/json"
)

var (
	illegalCharsRE = regexp.MustCompile(`[^a-zA-Z0-9_\-./]`)
)

type OpenTSDBClient struct {
	url        string
	httpClient *http.Client
}

func NewOpenTSDBClient(url string, timeout time.Duration) *OpenTSDBClient {
	return &OpenTSDBClient{
		url:        url,
		httpClient: utility.NewDeadlineClient(timeout),
	}
}

type StoreSamplesRequest struct {
	Metric    string                  `json:"metric"`
	Timestamp int64                   `json:"timestamp"`
	Value     clientmodel.SampleValue `json:"value"`
	Tags      map[string]string       `json:"tags"`
}

func escapeTagValue(l clientmodel.LabelValue) string {
	return illegalCharsRE.ReplaceAllString(string(l), "_")
}

func tagsFromMetric(m clientmodel.Metric) map[string]string {
	tags := map[string]string{}
	for l, v := range m {
		if l == clientmodel.MetricNameLabel {
			continue
		}
		tags[string(l)] = escapeTagValue(v)
	}
	return tags
}

var datapoints = 0

func (c *OpenTSDBClient) Store(samples clientmodel.Samples) error {
	datapoints += len(samples)
	glog.Infof("Successfully stored %d datapoints", datapoints)

	reqs := make([]StoreSamplesRequest, 0, len(samples))
	for _, s := range samples {
		metric := escapeTagValue(s.Metric[clientmodel.MetricNameLabel])
		reqs = append(reqs, StoreSamplesRequest{
			Metric:    metric,
			Timestamp: s.Timestamp.Unix(),
			Value:     s.Value,
			Tags:      tagsFromMetric(s.Metric),
		})
	}

	u, err := url.Parse(c.url)
	if err != nil {
		return err
	}

	u.Path = putEndpoint

	buf, err := json.Marshal(reqs)
	if err != nil {
		return err
	}
	startTime := time.Now()
	resp, err := c.httpClient.Post(
		u.String(),
		contentTypeJson,
		bytes.NewBuffer(buf),
	)
	glog.Infof("Took %s", time.Since(startTime))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	// API returns status code 204 for successful writes.
	if resp.StatusCode == http.StatusNoContent {
		glog.Infof("Successfully stored %d datapoints", len(samples))
		return nil
	}

	buf, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	glog.Warning(string(buf))
	var r map[string]int
	if err := json.Unmarshal(buf, &r); err != nil {
		return err
	}
	return fmt.Errorf("Failed to write %d samples to OpenTSDB, %d succeeded", r["failed"], r["success"])
}
