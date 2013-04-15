# Prometheus

Bedecke deinen Himmel, Zeus!  A new kid is in town.

Prometheus is a generic time series collection and computation server that is
useful in the following fields:

1. Industrial Experimentation / Real-Time Behavioral Validation / Software Release Qualification
2. Econometric and Natural Sciences
3. Operational Concerns and Monitoring

The system is designed to collect telemetry from named targets on given
intervals, evaluate rule expressions, display the results, and trigger an
action if some condition is observed to be true.

## Prerequisites

  1. Go 1.0.3.
  2. GVM: [https://github.com/moovweb/gvm](https://github.com/moovweb/gvm) is highly recommended as well.
  3. LevelDB: [https://code.google.com/p/leveldb/](https://code.google.com/p/leveldb/).
  4. Protocol Buffers Compiler: [http://code.google.com/p/protobuf/](http://code.google.com/p/protobuf/).
  5. goprotobuf: the code generator and runtime library: [http://code.google.com/p/goprotobuf/](http://code.google.com/p/goprotobuf/).
  6. Levigo, a Go-wrapper around LevelDB's C library: [https://github.com/jmhodges/levigo](https://github.com/jmhodges/levigo).
  7. GoRest, a RESTful style web-services framework: [http://code.google.com/p/gorest/](http://code.google.com/p/gorest/).
  8. Prometheus Client, Prometheus in Prometheus [https://github.com/prometheus/client_golang](https://github.com/prometheus/client_golang).
  9. Snappy, a compression library for LevelDB and Levigo [http://code.google.com/p/snappy/](http://code.google.com/p/snappy/).
 10. Protocol Buffers C Generator: [https://code.google.com/p/protobuf-c](https://code.google.com/p/protobuf-c).
 11. TCMalloc and Friends: [https://code.google.com/p/gperftools/](https://code.google.com/p/gperftools/).

## Getting Started

For basic help how to get started:

  * The source code is periodically indexed: [Prometheus Core](http://godoc.org/github.com/prometheus/prometheus).
  * For UNIX-like environment users users, please consult the Travis CI configuration in _.travis.yml_ and _Makefile.TRAVIS_.
  * All of the core developers are accessible via the [Prometheus Developers Mailinglist](https://groups.google.com/forum/?fromgroups#!forum/prometheus-developers).

### Linux
For first time users, simply run the following:

    make -f Makefile.TRAVIS

### Mac OS X
We have a handly [Getting started on Mac OS X](documentation/guides/getting-started-osx.md) guide.


### General

For subsequent users, feel free to run the various targets list in _Makefile_.

## Testing

[![Build Status](https://travis-ci.org/prometheus/prometheus.png)](https://travis-ci.org/prometheus/prometheus)

## License

Apache License 2.0
