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

# CXXFLAGS will minimally need to include the path to Google Protocol Buffers
# and Google Test and to the generated Protocol Buffer for the DTO models.

# LD_ASSETS will minimally need to include the path to Google Protocol Buffers
# and Google Test.  On Mac OS X, I was only able to make this succeed by using
# the static library artifacts.

MAKE_ARTIFACTS = libcomparer.a comparer.o ccomparer.o test_suite test_suite.o

all: test

test: test_suite

comparer.o: comparer.cpp comparer.hpp
	$(CXX) $(CXXFLAGS) comparer.cpp -c -o $@

# This target is especially problematic and should deserve attention.
ccomparer.o: comparer.o comparer.c comparer.h
	$(CXX) $(CXXFLAGS) comparer.c -c -o $@

libcomparer.a: comparer.o ccomparer.o ../model/generated/data.pb.o $(LD_ASSETS)
	-for i in $(LD_ASSETS) ; do ar x $${i} ; done
	ar rcs libcomparer.a comparer.o ccomparer.o ../model/generated/data.pb.o *.o

test_suite: libcomparer.a test_suite.o
	$(CXX) $(LDFLAGS) libcomparer.a test_suite.o -o test_suite
	./$@

test_suite.o: comparer_test.cpp
	$(CXX) $(CXXFLAGS) $< -c -o $@

clean:
	-rm -f $(MAKE_ARTIFACTS)
	rm -f *.o

.PHONY: clean generated test
