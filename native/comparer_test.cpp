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

#include <gtest/gtest.h>

#include <generated/data.pb.h>

#include "comparer.hpp"
#include "comparer.h"

TEST(CompareAnomaly, BothEmpty) {
  std::string left;
  std::string right;

  dto::SampleKey().SerializeToString(&left);
  dto::SampleKey().SerializeToString(&right);

  EXPECT_EQ(0, Compare(left.c_str(), left.size(), right.c_str(), right.size()));
  EXPECT_EQ(0, compare(NULL, left.c_str(), left.size(), right.c_str(), right.size()));
}

TEST(CompareAnomaly, LeftEmpty) {
  std::string left;
  std::string right;

  dto::SampleKey().SerializeToString(&left);
  dto::SampleKey r;
  dto::Fingerprint f = r.fingerprint();
  f.set_hash(1000);
  f.set_first("a");
  f.set_modulus(0);
  f.set_last("z");
  r.set_timestamp(1);
  r.set_last_timestamp(1);
  r.set_sample_count(1);
  r.SerializeToString(&right);

  EXPECT_EQ(-1, Compare(left.c_str(), left.size(), right.c_str(), right.size()));
  EXPECT_EQ(-1, compare(NULL, left.c_str(), left.size(), right.c_str(), right.size()));
}

TEST(CompareAnomaly, RightEmpty) {
  std::string left;
  std::string right;

  dto::SampleKey().SerializeToString(&right);
  dto::SampleKey l;
  dto::Fingerprint *f = l.mutable_fingerprint();
  f->set_hash(1000);
  f->set_first("a");
  f->set_modulus(0);
  f->set_last("z");
  l.set_timestamp(1);
  l.set_last_timestamp(1);
  l.set_sample_count(1);
  l.SerializeToString(&left);

  EXPECT_EQ(1, Compare(left.c_str(), left.size(), right.c_str(), right.size()));
  EXPECT_EQ(1, compare(NULL, left.c_str(), left.size(), right.c_str(), right.size()));
}

TEST(Compare, BothEqual) {
  std::string left;
  std::string right;

  dto::SampleKey both;
  dto::Fingerprint *f = both.mutable_fingerprint();
  f->set_hash(1000);
  f->set_first("a");
  f->set_modulus(0);
  f->set_last("z");
  both.set_timestamp(1);
  both.set_last_timestamp(1);
  both.set_sample_count(1);
  both.SerializeToString(&left);
  both.SerializeToString(&right);

  EXPECT_EQ(0, Compare(left.c_str(), left.size(), right.c_str(), right.size()));
  EXPECT_EQ(0, compare(NULL, left.c_str(), left.size(), right.c_str(), right.size()));
}

TEST(CompareBothEqual, NoLastTimestampSideEffects) {
  std::string left;
  std::string right;

  dto::SampleKey both;
  dto::Fingerprint *f = both.mutable_fingerprint();
  f->set_hash(1000);
  f->set_first("a");
  f->set_modulus(0);
  f->set_last("z");
  both.set_timestamp(1);
  both.set_last_timestamp(1);
  both.set_sample_count(1);
  both.SerializeToString(&left);
  both.set_last_timestamp(2);
  both.SerializeToString(&right);

  EXPECT_EQ(0, Compare(left.c_str(), left.size(), right.c_str(), right.size()));
  EXPECT_EQ(0, compare(NULL, left.c_str(), left.size(), right.c_str(), right.size()));
}

TEST(CompareBothEqual, NoSampleCountSideEffects) {
  std::string left;
  std::string right;

  dto::SampleKey both;
  dto::Fingerprint *f = both.mutable_fingerprint();
  f->set_hash(1000);
  f->set_first("a");
  f->set_modulus(0);
  f->set_last("z");
  both.set_timestamp(1);
  both.set_last_timestamp(1);
  both.set_sample_count(1);
  both.SerializeToString(&left);
  both.set_sample_count(2);
  both.SerializeToString(&right);

  EXPECT_EQ(0, Compare(left.c_str(), left.size(), right.c_str(), right.size()));
  EXPECT_EQ(0, compare(NULL, left.c_str(), left.size(), right.c_str(), right.size()));
}


TEST(CompareHash, SmallerLeft) {
  std::string left;
  std::string right;

  dto::SampleKey both;
  dto::Fingerprint *f = both.mutable_fingerprint();
  f->set_hash(999);
  f->set_first("a");
  f->set_modulus(0);
  f->set_last("z");
  both.set_timestamp(1);
  both.set_last_timestamp(1);
  both.set_sample_count(1);
  both.SerializeToString(&left);
  f->set_hash(1000);
  both.SerializeToString(&right);

  EXPECT_EQ(-1, Compare(left.c_str(), left.size(), right.c_str(), right.size()));
  EXPECT_EQ(-1, compare(NULL, left.c_str(), left.size(), right.c_str(), right.size()));
}

TEST(CompareHash, SmallerRight) {
  std::string left;
  std::string right;

  dto::SampleKey both;
  dto::Fingerprint *f = both.mutable_fingerprint();
  f->set_hash(1000);
  f->set_first("a");
  f->set_modulus(0);
  f->set_last("z");
  both.set_timestamp(1);
  both.set_last_timestamp(1);
  both.set_sample_count(1);
  both.SerializeToString(&left);
  f->set_hash(999);
  both.SerializeToString(&right);

  EXPECT_EQ(1, Compare(left.c_str(), left.size(), right.c_str(), right.size()));
  EXPECT_EQ(1, compare(NULL, left.c_str(), left.size(), right.c_str(), right.size()));
}

TEST(CompareFirst, SmallerLeft) {
  std::string left;
  std::string right;

  dto::SampleKey both;
  dto::Fingerprint *f = both.mutable_fingerprint();
  f->set_hash(1000);
  f->set_first("a");
  f->set_modulus(0);
  f->set_last("z");
  both.set_timestamp(1);
  both.set_last_timestamp(1);
  both.set_sample_count(1);
  both.SerializeToString(&left);
  f->set_first("b");
  both.SerializeToString(&right);

  EXPECT_EQ(-1, Compare(left.c_str(), left.size(), right.c_str(), right.size()));
  EXPECT_EQ(-1, compare(NULL, left.c_str(), left.size(), right.c_str(), right.size()));
}

TEST(CompareFirst, SmallerRight) {
  std::string left;
  std::string right;

  dto::SampleKey both;
  dto::Fingerprint *f = both.mutable_fingerprint();
  f->set_hash(1000);
  f->set_first("b");
  f->set_modulus(0);
  f->set_last("z");
  both.set_timestamp(1);
  both.set_last_timestamp(1);
  both.set_sample_count(1);
  both.SerializeToString(&left);
  f->set_first("a");
  both.SerializeToString(&right);

  EXPECT_EQ(1, Compare(left.c_str(), left.size(), right.c_str(), right.size()));
  EXPECT_EQ(1, compare(NULL, left.c_str(), left.size(), right.c_str(), right.size()));
}

TEST(CompareModulus, SmallerLeft) {
  std::string left;
  std::string right;

  dto::SampleKey both;
  dto::Fingerprint *f = both.mutable_fingerprint();
  f->set_hash(1000);
  f->set_first("a");
  f->set_modulus(0);
  f->set_last("z");
  both.set_timestamp(1);
  both.set_last_timestamp(1);
  both.set_sample_count(1);
  both.SerializeToString(&left);
  f->set_modulus(1);
  both.SerializeToString(&right);

  EXPECT_EQ(-1, Compare(left.c_str(), left.size(), right.c_str(), right.size()));
  EXPECT_EQ(-1, compare(NULL, left.c_str(), left.size(), right.c_str(), right.size()));
}

TEST(CompareModulus, SmallerRight) {
  std::string left;
  std::string right;

  dto::SampleKey both;
  dto::Fingerprint *f = both.mutable_fingerprint();
  f->set_hash(1000);
  f->set_first("a");
  f->set_modulus(1);
  f->set_last("z");
  both.set_timestamp(1);
  both.set_last_timestamp(1);
  both.set_sample_count(1);
  both.SerializeToString(&left);
  f->set_modulus(0);
  both.SerializeToString(&right);

  EXPECT_EQ(1, Compare(left.c_str(), left.size(), right.c_str(), right.size()));
  EXPECT_EQ(1, compare(NULL, left.c_str(), left.size(), right.c_str(), right.size()));
}

TEST(CompareLast, SmallerLeft) {
  std::string left;
  std::string right;

  dto::SampleKey both;
  dto::Fingerprint *f = both.mutable_fingerprint();
  f->set_hash(1000);
  f->set_first("a");
  f->set_modulus(0);
  f->set_last("y");
  both.set_timestamp(1);
  both.set_last_timestamp(1);
  both.set_sample_count(1);
  both.SerializeToString(&left);
  f->set_last("z");
  both.SerializeToString(&right);

  EXPECT_EQ(-1, Compare(left.c_str(), left.size(), right.c_str(), right.size()));
  EXPECT_EQ(-1, compare(NULL, left.c_str(), left.size(), right.c_str(), right.size()));
}

TEST(CompareLast, SmallerRight) {
  std::string left;
  std::string right;

  dto::SampleKey both;
  dto::Fingerprint *f = both.mutable_fingerprint();
  f->set_hash(1000);
  f->set_first("a");
  f->set_modulus(0);
  f->set_last("z");
  both.set_timestamp(1);
  both.set_last_timestamp(1);
  both.set_sample_count(1);
  both.SerializeToString(&left);
  f->set_last("y");
  both.SerializeToString(&right);

  EXPECT_EQ(1, Compare(left.c_str(), left.size(), right.c_str(), right.size()));
  EXPECT_EQ(1, compare(NULL, left.c_str(), left.size(), right.c_str(), right.size()));
}

TEST(CompareTimestamp, SmallerLeft) {
  std::string left;
  std::string right;

  dto::SampleKey both;
  dto::Fingerprint *f = both.mutable_fingerprint();
  f->set_hash(1000);
  f->set_first("a");
  f->set_modulus(0);
  f->set_last("z");
  both.set_timestamp(1);
  both.set_last_timestamp(1);
  both.set_sample_count(1);
  both.SerializeToString(&left);
  both.set_timestamp(2);
  both.SerializeToString(&right);

  EXPECT_EQ(-1, Compare(left.c_str(), left.size(), right.c_str(), right.size()));
  EXPECT_EQ(-1, compare(NULL, left.c_str(), left.size(), right.c_str(), right.size()));
}

TEST(CompareTimestamp, SmallerRight) {
  std::string left;
  std::string right;

  dto::SampleKey both;
  dto::Fingerprint *f = both.mutable_fingerprint();
  f->set_hash(1000);
  f->set_first("a");
  f->set_modulus(0);
  f->set_last("z");
  both.set_timestamp(2);
  both.set_last_timestamp(1);
  both.set_sample_count(1);
  both.SerializeToString(&left);
  both.set_timestamp(1);
  both.SerializeToString(&right);

  EXPECT_EQ(1, Compare(left.c_str(), left.size(), right.c_str(), right.size()));
  EXPECT_EQ(1, compare(NULL, left.c_str(), left.size(), right.c_str(), right.size()));
}

TEST(Protocol, Compare) {
  int (*f)(void*, const char*, size_t, const char*, size_t) = &compare;

  EXPECT_FALSE(f == NULL);
}

TEST(Protocol, Name) {
  const char* (*f)(void*) = &name;

  EXPECT_FALSE(f == NULL);

  EXPECT_STREQ("SampleKeyComparator", f(NULL));
}

TEST(Protocol, Destroy) {
  void (*f)(void*) = &destroy;

  EXPECT_FALSE(f == NULL);

  f(NULL);
}

TEST(Protocol, New) {
  leveldb_comparator_t *comparator = new_comparator();

  EXPECT_FALSE(comparator == NULL);

  leveldb_comparator_destroy(comparator);
}
