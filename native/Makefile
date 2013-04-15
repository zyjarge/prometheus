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

include ../Makefile.INCLUDE

MAKE_ARTIFACTS = comparer.o ccomparer.o test_suite test_suite.o

export CXXFLAGS += -Wall -Werror
export CFLAGS += -Wall -Werror

export CGO_LDFLAGS := $(LDFLAGS) ../model/generated/data.pb.o

all: test

test: test_suite
	go test -v -x -work .

cppcomparer.o: comparer.cpp comparer.hpp
	$(CXX) $(CXXFLAGS) comparer.cpp -c -o $@

ccomparer.o: comparer.hpp comparer.c comparer.h
	$(CC) $(CFLAGS) comparer.c -c -o $@

test_suite: cppcomparer.o ccomparer.o test_suite.o
	$(CXX) $(LDFLAGS) -lstdc++ cppcomparer.o ccomparer.o test_suite.o ../model/generated/data.pb.o $(GOPATH)/lib/libgtest.a $(GOPATH)/lib/libgtest_main.a $(GOPATH)/lib/libleveldb.a $(GOPATH)/lib/libprotobuf.a $(GOPATH)/lib/libsnappy.a $(GOPATH)/lib/libtcmalloc.a -o test_suite
	./$@

test_suite.o: comparer_test.cpp
	$(CXX) $(CXXFLAGS) $< -c -o $@

clean:
	-rm -f $(MAKE_ARTIFACTS)
	rm -f *.o

.PHONY: clean generated test
