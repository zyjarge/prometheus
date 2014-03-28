package opentsdb

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"time"

	"github.com/golang/glog"
	clientmodel "github.com/prometheus/client_golang/model"
	"github.com/prometheus/prometheus/utility"
)

const (
	putEndpoint     = "/api/put"
	getEndpoint     = "/api/query"
	assignEndpoint  = "/api/uid/assign"
	contentTypeJSON = "application/json"
)

var (
	illegalCharsRE = regexp.MustCompile(`[^a-zA-Z0-9_\-./]`)
)

// Client allows sending batches of Prometheus samples to OpenTSDB.
type Client struct {
	url        string
	httpClient *http.Client

	// TODO(bjoern): Replace the following three by LRU caches.
	metricNameUIDs TagValueToStringMap
	tagKeyUIDs     map[string]string
	tagValueUIDs   TagValueToStringMap
}

// NewClient creates a new Client.
func NewClient(url string, timeout time.Duration) *Client {
	return &Client{
		url:            url,
		httpClient:     utility.NewDeadlineClient(timeout),
		metricNameUIDs: TagValueToStringMap{},
		tagKeyUIDs:     map[string]string{},
		tagValueUIDs:   TagValueToStringMap{},
	}
}

// StoreSamplesRequest is used for building a JSON request for storing samples
// via the OpenTSDB.
type StoreSamplesRequest struct {
	MetricName TagValue            `json:"metric"`
	Timestamp  int64               `json:"timestamp"`
	Value      float64             `json:"value"`
	Tags       map[string]TagValue `json:"tags"`
}

// GetSamplesResponse represents the JSON response sent by OpenTSDB upon getting
// time series.
type GetSamplesResponse struct {
	MetricName    TagValue            `json:"metric"`
	Tags          map[string]TagValue `json:"tags"`
	AggregateTags []string            `json:"aggregateTags"`
	DPs           map[string]float64  `json:"dps"`
}

// AssignRequest is used for building a JSON request to find out about UIDs in
// OpenTSDB.
type AssignRequest struct {
	MetricNames []TagValue `json:"metric"`
	TagKeys     []string   `json:"tagk"`
	TagValues   []TagValue `json:"tagv"`
}

// MakeAssignRequest creates an AssignRequest with empty slices for each field.
func MakeAssignRequest() AssignRequest {
	return AssignRequest{
		MetricNames: []TagValue{},
		TagKeys:     []string{},
		TagValues:   []TagValue{},
	}
}

// AssignResponse represents the JSON repsonse sent by OpenTSDB to tell us about
// UIDs.
type AssignResponse struct {
	MetricNames      TagValueToStringMap `json:"metric"`
	MetricNameErrors TagValueToStringMap `json:"metric_errors"`
	TagKeys          map[string]string   `json:"tagk"`
	TagKeyErrors     map[string]string   `json:"tagk_errors"`
	TagValues        TagValueToStringMap `json:"tagv"`
	TagValueErrors   TagValueToStringMap `json:"tagv_errors"`
}

// tagsFromMetric translates Prometheus metric into OpenTSDB tags.
func tagsFromMetric(m clientmodel.Metric) map[string]TagValue {
	tags := make(map[string]TagValue, len(m)-1)
	for l, v := range m {
		if l == clientmodel.MetricNameLabel {
			continue
		}
		tags[string(l)] = TagValue(v)
	}
	return tags
}

// Store sends a batch of samples to OpenTSDB via its HTTP API.
func (c *Client) Store(samples clientmodel.Samples) error {
	reqs := make([]StoreSamplesRequest, 0, len(samples))
	for _, s := range samples {
		v := float64(s.Value)
		if math.IsNaN(v) || math.IsInf(v, 0) {
			glog.Warningf("cannot send value %d to OpenTSDB, skipping sample %#v", v, s)
			continue
		}
		metric := TagValue(s.Metric[clientmodel.MetricNameLabel])
		reqs = append(reqs, StoreSamplesRequest{
			MetricName: metric,
			Timestamp:  s.Timestamp.Unix(),
			Value:      v,
			Tags:       tagsFromMetric(s.Metric),
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

	resp, err := c.httpClient.Post(
		u.String(),
		contentTypeJSON,
		bytes.NewBuffer(buf),
	)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	// API returns status code 204 for successful writes.
	// http://opentsdb.net/docs/build/html/api_http/put.html
	if resp.StatusCode == http.StatusNoContent {
		return nil
	}

	// API returns status code 400 on error, encoding error details in the
	// response content in JSON.
	buf, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	var r map[string]int
	if err := json.Unmarshal(buf, &r); err != nil {
		return err
	}
	return fmt.Errorf("failed to write %d samples to OpenTSDB, %d succeeded", r["failed"], r["success"])
}

// Retrieve retrieves all samples of a time series within the given time
// interval from OpenTSDB via its HTTP API.
func (c *Client) Retrieve(
	metric clientmodel.Metric,
	startTime clientmodel.Timestamp,
	endTime clientmodel.Timestamp,
) (clientmodel.Samples, error) {

	u, err := url.Parse(c.url)
	if err != nil {
		return nil, err
	}

	u.Path = getEndpoint

	params := url.Values{}
	tsuid, err := c.getTSUID(metric)
	if err != nil {
		return nil, err
	}
	params.Add("tsuid", "sum:"+tsuid)
	params.Add("start", fmt.Sprint(startTime))
	params.Add("end", fmt.Sprint(endTime))
	u.RawQuery = params.Encode()

	resp, err := c.httpClient.Get(u.String())
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	buf, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		// TODO(bjoern): Unmarshal the JSON and pull out the 'proper' error message.
		return nil, fmt.Errorf(
			"status code %d retrieving data from OpenTSDB: %s",
			resp.StatusCode,
			buf,
		)
	}
	var tss []GetSamplesResponse
	if err := json.Unmarshal(buf, &tss); err != nil {
		return nil, err
	}
	if len(tss) == 0 {
		return clientmodel.Samples{}, nil
	}
	if len(tss) != 1 {
		return nil, fmt.Errorf("expected exactly 1 time series returned from OpenTSDB, got %d", len(tss))
	}
	ts := tss[0]
	result := make(clientmodel.Samples, 0, len(ts.DPs))
	for k, v := range ts.DPs {
		timestamp, err := strconv.ParseInt(k, 10, 64)
		if err != nil {
			return nil, err
		}
		result = append(
			result,
			&clientmodel.Sample{
				Metric:    metric,
				Value:     clientmodel.SampleValue(v),
				Timestamp: clientmodel.Timestamp(timestamp),
			},
		)
	}
	sort.Sort(result)
	return result, nil
}

func (c *Client) getTSUID(m clientmodel.Metric) (string, error) {
	// Gather the metric names, tag keys, and tag values for which no UID is cached.
	req := MakeAssignRequest()
	for n, v := range m {
		if n == clientmodel.MetricNameLabel {
			if _, ok := c.metricNameUIDs[TagValue(v)]; !ok {
				req.MetricNames = append(req.MetricNames, TagValue(v))
			}
			continue
		}
		if _, ok := c.tagKeyUIDs[string(n)]; !ok {
			req.TagKeys = append(req.TagKeys, string(n))
		}
		if _, ok := c.tagValueUIDs[TagValue(v)]; !ok {
			req.TagValues = append(req.TagValues, TagValue(v))
		}
	}

	// Request missing UIDs from OpenTSDB.
	if len(req.MetricNames)+len(req.TagKeys)+len(req.TagValues) > 0 {
		u, err := url.Parse(c.url)
		if err != nil {
			return "", err
		}
		u.Path = assignEndpoint

		jsonReq, err := json.Marshal(req)
		if err != nil {
			return "", err
		}

		httpResp, err := c.httpClient.Post(
			u.String(),
			contentTypeJSON,
			bytes.NewBuffer(jsonReq),
		)
		if err != nil {
			return "", err
		}
		defer httpResp.Body.Close()
		// We deliberately ignore the status code. A 400 is actually expected
		// (but not guaranteed...). This is dirty stuff...
		jsonResp, err := ioutil.ReadAll(httpResp.Body)
		if err != nil {
			return "", err
		}
		var resp AssignResponse
		if err := json.Unmarshal(jsonResp, &resp); err != nil {
			return "", err
		}
		// As said, this is dirty. We will extract UIDs from error messages, too...
		// TODO(bjoern): Once API v2.1 is there, this can be done in a clean way.
		for tv, uid := range resp.MetricNames {
			c.metricNameUIDs[tv] = uid
		}
		for tv, err := range resp.MetricNameErrors {
			c.metricNameUIDs[tv] = err[len(err)-6:]
		}
		for k, uid := range resp.TagKeys {
			c.tagKeyUIDs[k] = uid
		}
		for k, err := range resp.TagKeyErrors {
			c.tagKeyUIDs[k] = err[len(err)-6:]
		}
		for tv, uid := range resp.TagValues {
			c.tagValueUIDs[tv] = uid
		}
		for tv, err := range resp.TagValueErrors {
			c.tagValueUIDs[tv] = err[len(err)-6:]
		}
	}

	// Assemble the TSUID.
	tsuid := bytes.NewBuffer(make([]byte, 0, len(m)*12-6))
	tsuid.WriteString(c.metricNameUIDs[TagValue(m[clientmodel.MetricNameLabel])])
	tagUIDPairs := make([]string, 0, len(m)-1)
	for n, v := range m {
		if n != clientmodel.MetricNameLabel {
			tagUIDPairs = append(tagUIDPairs, c.tagKeyUIDs[string(n)]+c.tagValueUIDs[TagValue(v)])
		}
	}
	sort.Strings(tagUIDPairs)
	for _, t := range tagUIDPairs {
		tsuid.WriteString(t)
	}
	return tsuid.String(), nil
}
