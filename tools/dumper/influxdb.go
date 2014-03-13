package main

import (
	"github.com/golang/glog"

	idb "github.com/influxdb/influxdb-go"
)

func newInfluxDB() *idb.Client {
	config := &idb.ClientConfig{
		Host:     "localhost:8086",
		Username: "root",
		Password: "root",
		Database: "testdb",
	}
	c, err := idb.NewClient(config)
	if err != nil {
		glog.Fatal(err)
	}
	return c
}
