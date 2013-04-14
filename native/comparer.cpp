// Copyright 2013 Prometheus Team
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <string>

#include <generated/data.pb.h>

#ifdef __cplusplus
extern "C" {
#endif

const int Compare(const char *l, size_t llen,
                  const char *r, size_t rlen) {
  dto::SampleKey left;
  dto::SampleKey right;

  left.ParseFromString(std::string(l, llen));
  right.ParseFromString(std::string(r, rlen));

  if (left.fingerprint().hash() < right.fingerprint().hash()) {
    return -1;
  } else if (left.fingerprint().hash() > right.fingerprint().hash()) {
    return 1;
  }

  switch (left.fingerprint().first().compare(right.fingerprint().first())) {
  case -1:
    return -1;
  case 1:
    return 1;
  }

  if (left.fingerprint().modulus() < right.fingerprint().modulus()) {
    return -1;
  } else if (left.fingerprint().modulus() > right.fingerprint().modulus()) {
    return 1;
  }

  switch (left.fingerprint().last().compare(right.fingerprint().last())) {
  case -1:
    return -1;
  case 1:
    return 1;
  }

  if (left.timestamp() < right.timestamp()) {
    return -1;
  } else if (left.timestamp() > right.timestamp()) {
    return 1;
  }

  return 0;
}

#ifdef __cplusplus
}
#endif
