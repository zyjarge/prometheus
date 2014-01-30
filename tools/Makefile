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

all: compactor dumper populator pruner querier rule_checker

SUFFIXES:

include ../Makefile.INCLUDE

compactor:
	$(MAKE) -C compactor

dumper:
	$(MAKE) -C dumper

populator:
	$(MAKE) -C populator

pruner:
	$(MAKE) -C pruner

querier:
	$(MAKE) -C querier

rule_checker:
	$(MAKE) -C rule_checker

clean:
	$(MAKE) -C compactor clean
	$(MAKE) -C dumper clean
	$(MAKE) -C populator clean
	$(MAKE) -C pruner clean
	$(MAKE) -C querier clean
	$(MAKE) -C rule_checker clean

.PHONY: clean compactor dumper populator pruner querier rule_checker
