package tsdb

import (
	"github.com/golang/glog"

	clientmodel "github.com/prometheus/client_golang/model"
)

const (
	maxSamplesPerRequest = 100
)

type TSDBClient interface {
	Store(clientmodel.Samples) error
}

type TSDBQueueManager struct {
	tsdb  TSDBClient
	queue chan clientmodel.Samples
	stop  chan bool
}

func NewTSDBQueueManager(tsdb TSDBClient, queueCapacity int) *TSDBQueueManager {
	return &TSDBQueueManager{
		tsdb:  tsdb,
		queue: make(chan clientmodel.Samples, queueCapacity),
	}
}

func (t *TSDBQueueManager) Store(s clientmodel.Samples) {
	if len(t.queue) == cap(t.queue) {
		glog.Warningf("TSDB queue full, dropping %d samples on the floor", s)
		return
	}
	t.queue <- s
}

func (t *TSDBQueueManager) sendSamples(s clientmodel.Samples) {
	// Samples are sent to the TSDB on a best-effort basis. If a sample isn't
	// sent correctly the first time, it's simply dropped on the floor.
	if err := t.tsdb.Store(s); err != nil {
		glog.Warningf("error sending %d samples to TSDB: %s", len(s), err)
	}
}

func (t *TSDBQueueManager) Run() {
	for s := range t.queue {
		// Send chunks of at most maxSamplesPerRequest samples to the TSDB.
		for len(s) > maxSamplesPerRequest {
			t.sendSamples(s[:maxSamplesPerRequest])
			s = s[maxSamplesPerRequest:]
		}
		if len(s) > 0 {
			go t.sendSamples(s)
		}
	}
}

func (t *TSDBQueueManager) Close() {
	close(t.queue)
}
