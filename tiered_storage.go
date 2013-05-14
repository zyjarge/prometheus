package main

import (
	"flag"
	"github.com/prometheus/prometheus/storage/metric"
	"log"
	"time"
)

type storageSettings struct {
	FlushInterval  *time.Duration
	WriteInterval  *time.Duration
	TTL            *time.Duration
	DiskQueueDepth *uint
	Path           *string
	ViewQueueDepth *uint
}

var StorageSettings = storageSettings{
	flag.Duration("storage.memory.flushInterval", time.Second*30, "how often to flush memory samples to disk"),
	flag.Duration("storage.memory.writeInterval", time.Second*1, "how often to write samples to disk [deprecated]"),
	flag.Duration("storage.memory.ttl", time.Second*20, "age of flushed samples"),
	flag.Uint("storage.disk.queueDepth", 1000000, "maximum pending writes"),
	flag.String("storage.disk.path", "/tmp/metrics", "base path for metrics storage"),
	flag.Uint("storage.view.queueDepth", 100, "maximum pending views (?)"),
}

func MustNewTieredStorage(settings storageSettings) *metric.TieredStorage {
	ts, err := metric.NewTieredStorage(
		*settings.DiskQueueDepth,
		*settings.ViewQueueDepth,
		*settings.FlushInterval,
		*settings.WriteInterval,
		*settings.TTL,
		*settings.Path,
	)

	if err != nil {
		log.Fatalf("Error opening storage: %s", err)
	}

	if ts == nil {
		log.Fatal("nil tiered storage")
	}

	return ts
}
