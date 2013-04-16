# Copyright 2013 Prometheus Team
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

.SUFFIXES:

TEST_ARTIFACTS = prometheus prometheus.build search_index

include Makefile.INCLUDE

export CGO_CFLAGS = $(CFLAGS)
export CGO_LDFLAGS = -lprotobuf-c $(LDFLAGS)

all: test

test: build
	$(MAKE) -C native test
	go test ./appstate/... $(GO_TEST_FLAGS)
	go test ./coding/... $(GO_TEST_FLAGS)
	go test ./config/... $(GO_TEST_FLAGS)
	go test ./model/... $(GO_TEST_FLAGS)
	go test ./retrieval/... $(GO_TEST_FLAGS)
	go test ./rules/... $(GO_TEST_FLAGS)
	go test ./storage/... $(GO_TEST_FLAGS)
	go test ./utility/... $(GO_TEST_FLAGS)
	go test ./web/... $(GO_TEST_FLAGS)

model:
	$(MAKE) -C model

native: model
	$(MAKE) -C native

build: model native
	$(MAKE) -C web
	go build .

binary: build
	go build -o prometheus.build

clean:
	$(MAKE) -C build clean
	$(MAKE) -C model clean
	$(MAKE) -C native clean
	$(MAKE) -C web clean
	rm -rf $(TEST_ARTIFACTS)
	-find . -type f -iname '*~' -exec rm '{}' ';'
	-find . -type f -iname '*#' -exec rm '{}' ';'
	-find . -type f -iname '.#*' -exec rm '{}' ';'

format:
	find . -iname '*.go' | egrep -v "generated|\.(l|y)\.go" | xargs -n1 gofmt -w -s=true

advice:
	go tool vet .

search_index:
	godoc -index -write_index -index_files='search_index'

documentation: search_index
	godoc -http=:6060 -index -index_files='search_index'

.PHONY: advice binary build clean documentation format model native search_index test
