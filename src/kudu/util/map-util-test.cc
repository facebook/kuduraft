// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// This unit test belongs in gutil, but it depends on test_main which is
// part of util.
#include "kudu/gutil/map-util.h"

#include <map>
#include <memory>
#include <string>
#include <utility>

#include <gtest/gtest.h>

using std::map;
using std::shared_ptr;
using std::string;
using std::unique_ptr;

namespace kudu {

TEST(FloorTest, TestMapUtil) {
  map<int, int> myMap;

  ASSERT_EQ(nullptr, FindFloorOrNull(myMap, 5));

  myMap[5] = 5;
  ASSERT_EQ(5, *FindFloorOrNull(myMap, 6));
  ASSERT_EQ(5, *FindFloorOrNull(myMap, 5));
  ASSERT_EQ(nullptr, FindFloorOrNull(myMap, 4));

  myMap[1] = 1;
  ASSERT_EQ(5, *FindFloorOrNull(myMap, 6));
  ASSERT_EQ(5, *FindFloorOrNull(myMap, 5));
  ASSERT_EQ(1, *FindFloorOrNull(myMap, 4));
  ASSERT_EQ(1, *FindFloorOrNull(myMap, 1));
  ASSERT_EQ(nullptr, FindFloorOrNull(myMap, 0));
}

TEST(ComputeIfAbsentTest, TestComputeIfAbsent) {
  map<string, string> myMap;
  auto result = ComputeIfAbsent(&myMap, "key", [] { return "hello_world"; });
  ASSERT_EQ(*result, "hello_world");
  auto result2 = ComputeIfAbsent(&myMap, "key", [] { return "hello_world2"; });
  ASSERT_EQ(*result2, "hello_world");
}

TEST(ComputeIfAbsentTest, TestComputeIfAbsentAndReturnAbsense) {
  map<string, string> myMap;
  auto result =
      ComputeIfAbsentReturnAbsense(&myMap, "key", [] { return "hello_world"; });
  ASSERT_TRUE(result.second);
  ASSERT_EQ(*result.first, "hello_world");
  auto result2 = ComputeIfAbsentReturnAbsense(
      &myMap, "key", [] { return "hello_world2"; });
  ASSERT_FALSE(result2.second);
  ASSERT_EQ(*result2.first, "hello_world");
}

TEST(FindPointeeOrNullTest, TestFindPointeeOrNull) {
  map<string, unique_ptr<string>> myMap;
  auto iter =
      myMap.emplace("key", unique_ptr<string>(new string("hello_world")));
  ASSERT_TRUE(iter.second);
  string* value = FindPointeeOrNull(myMap, "key");
  ASSERT_TRUE(value != nullptr);
  ASSERT_EQ(*value, "hello_world");
  myMap.erase(iter.first);
  value = FindPointeeOrNull(myMap, "key");
  ASSERT_TRUE(value == nullptr);
}

TEST(EraseKeyReturnValuePtrTest, TestRawAndSmartSmartPointers) {
  map<string, unique_ptr<string>> myMap;
  unique_ptr<string> value = EraseKeyReturnValuePtr(&myMap, "key");
  ASSERT_TRUE(value.get() == nullptr);
  myMap.emplace("key", unique_ptr<string>(new string("hello_world")));
  value = EraseKeyReturnValuePtr(&myMap, "key");
  ASSERT_EQ(*value, "hello_world");
  value.reset();
  value = EraseKeyReturnValuePtr(&myMap, "key");
  ASSERT_TRUE(value.get() == nullptr);
  map<string, shared_ptr<string>> myMap2;
  shared_ptr<string> value2 = EraseKeyReturnValuePtr(&myMap2, "key");
  ASSERT_TRUE(value2.get() == nullptr);
  myMap2.emplace("key", std::make_shared<string>("hello_world"));
  value2 = EraseKeyReturnValuePtr(&myMap2, "key");
  ASSERT_EQ(*value2, "hello_world");
  map<string, string*> myMapRaw;
  myMapRaw.emplace("key", new string("hello_world"));
  value.reset(EraseKeyReturnValuePtr(&myMapRaw, "key"));
  ASSERT_EQ(*value, "hello_world");
}

TEST(EmplaceTest, TestEmplace) {
  string key1("k");
  string key2("k2");
  // Map with move-only value type.
  map<string, unique_ptr<string>> myMap;
  unique_ptr<string> val(new string("foo"));
  ASSERT_TRUE(EmplaceIfNotPresent(&myMap, key1, std::move(val)));
  ASSERT_TRUE(myMap.contains(key1));
  ASSERT_FALSE(EmplaceIfNotPresent(&myMap, key1, nullptr))
      << "Should return false for already-present";

  val = unique_ptr<string>(new string("bar"));
  ASSERT_TRUE(EmplaceOrUpdate(&myMap, key2, std::move(val)));
  ASSERT_TRUE(myMap.contains(key2));
  auto it = myMap.find(key2);
  CHECK(it != myMap.end()) << "Map key not found: " << key2;
  ASSERT_EQ("bar", *it->second);
  val = unique_ptr<string>(new string("foobar"));
  ASSERT_FALSE(EmplaceOrUpdate(&myMap, key2, std::move(val)));
  auto it2 = myMap.find(key2);
  CHECK(it2 != myMap.end()) << "Map key not found: " << key2;
  ASSERT_EQ("foobar", *it2->second);
}

TEST(LookupOrEmplaceTest, IntMap) {
  const string key = "mega";
  map<string, int> intMap;

  {
    const auto& val = LookupOrEmplace(&intMap, key, 0);
    ASSERT_EQ(0, val);
    auto* valPtr = FindOrNull(intMap, key);
    ASSERT_NE(nullptr, valPtr);
    ASSERT_EQ(0, *valPtr);
  }

  {
    auto& val = LookupOrEmplace(&intMap, key, 10);
    ASSERT_EQ(0, val);
    ++val;
    auto* valPtr = FindOrNull(intMap, key);
    ASSERT_NE(nullptr, valPtr);
    ASSERT_EQ(1, *valPtr);
  }

  {
    LookupOrEmplace(&intMap, key, 100) += 1000;
    auto* valPtr = FindOrNull(intMap, key);
    ASSERT_NE(nullptr, valPtr);
    ASSERT_EQ(1001, *valPtr);
  }
}

TEST(LookupOrEmplaceTest, UniquePtrMap) {
  constexpr int key = 0;
  const string refStr = "turbo";
  map<int, unique_ptr<string>> uptrMap;

  {
    unique_ptr<string> val(new string(refStr));
    const auto& lookupVal = LookupOrEmplace(&uptrMap, key, std::move(val));
    ASSERT_EQ(nullptr, val.get());
    ASSERT_NE(nullptr, lookupVal.get());
    ASSERT_EQ(refStr, *lookupVal);
  }

  {
    unique_ptr<string> val(new string("giga"));
    auto& lookupVal = LookupOrEmplace(&uptrMap, key, std::move(val));
    ASSERT_NE(nullptr, lookupVal.get());
    ASSERT_EQ(refStr, *lookupVal);
    // Update the stored value.
    *lookupVal = "giga";
  }

  {
    unique_ptr<string> val(new string(refStr));
    const auto& lookupVal = LookupOrEmplace(&uptrMap, key, std::move(val));
    ASSERT_NE(nullptr, lookupVal.get());
    ASSERT_EQ("giga", *lookupVal);
  }
}

} // namespace kudu
