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

#ifndef COMPARER_HPP
#define COMPARER_HPP

#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

// Compare examines to byte buffers that contain serialized SampleKey entities
// and decides how to sort them.
//
// The basis for sorting is ascending determined by the the following elements
// in order of priority in cases of equality:
//
// 1. Fingerprint Hash (numeric)
// 2. Fingerprint First Label Name Character (lexicographic)
// 3. Fingerprint Label Matter Length Modulus (numeric)
// 4. Fingerprint Last Label Value Character (lexicographic)
// 5. Sample Super Time (numeric)
//
// This is implemented in C++ to avoid introducing extra C-based Protocol
// Buffer dependencies into the runtime.  Unfortunately for direct Go bindings,
// C must be used.  comparer.h provides this interface.
const int Compare(const char * left, size_t left_length,
                  const char * right, size_t right_length);

#ifdef __cplusplus
}
#endif

#endif
