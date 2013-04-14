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

#ifndef COMPARER_H
#define COMPARER_H

#include <stddef.h>

#define COMPARATOR_NAME "SampleKeyComparator"

#ifdef __cplusplus
extern "C" {
#endif

// compare merely proxies back to the C++ Compare.
//
// This is implemented in C due to limitations in Go's foreign function
// interface.  It may be possible, though I didn't succeed on my first try,
// to implement this comparator in Go and expose it back to the C layer via the
// "C" utility package.  This seems possible but could introduce circular
// dependencies.  Secondarily, for something as performance critical as this
// piece, it would be well to avoid the garbage collector.
//
// Another possibility, albeit more onerous, could be to implement a pure
// C++ LevelDB custom comparator class (leveldb::Comparator), but this is
// already tricky, as bindings in C would need to be provided therefore.  It is
// hard to say whether it would be possible in this case to provide the key
// prefix shortenings, which is an acknowledged limitation of the C language
// bridge for LevelDB.
//
// https://code.google.com/p/leveldb/source/browse/include/leveldb/c.h#215
int compare(void *unused, const char *l, size_t llen,
            const char *r, size_t rlen);

// name emits the name of this comparator.
//
// https://code.google.com/p/leveldb/source/browse/include/leveldb/c.h#219
const char* name(void *unused);

// destroy reaps any state associated with the comparator.
//
// https://code.google.com/p/leveldb/source/browse/include/leveldb/c.h#226
void destroy(void *unused);
#ifdef __cplusplus
}
#endif

#endif
