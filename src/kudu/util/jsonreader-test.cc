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

#include "kudu/util/jsonreader.h"

#include <cstdint>
#include <string>
#include <vector>

#include <gtest/gtest.h>
#include <rapidjson/document.h>

#include <fmt/core.h>
#include "kudu/util/status.h"
#include "kudu/util/test_macros.h"

using rapidjson::Value;
using std::string;
using std::vector;

namespace kudu {

TEST(JsonReaderTest, Corrupt) {
  JsonReader r("");
  Status s = r.init();
  ASSERT_TRUE(s.IsCorruption());
  ASSERT_STR_CONTAINS(s.ToString(), "JSON text is corrupt");
}

TEST(JsonReaderTest, Empty) {
  JsonReader r("{}");
  ASSERT_OK(r.init());
  JsonReader r2("[]");
  ASSERT_OK(r2.init());

  // Not found.
  ASSERT_TRUE(r.extractBool(r.root(), "foo", nullptr).IsNotFound());
  ASSERT_TRUE(r.extractInt32(r.root(), "foo", nullptr).IsNotFound());
  ASSERT_TRUE(r.extractInt64(r.root(), "foo", nullptr).IsNotFound());
  ASSERT_TRUE(r.extractString(r.root(), "foo", nullptr).IsNotFound());
  ASSERT_TRUE(r.extractObject(r.root(), "foo", nullptr).IsNotFound());
  ASSERT_TRUE(r.extractObjectArray(r.root(), "foo", nullptr).IsNotFound());
}

TEST(JsonReaderTest, Basic) {
  JsonReader r("{ \"foo\" : \"bar\" }");
  ASSERT_OK(r.init());
  string foo;
  ASSERT_OK(r.extractString(r.root(), "foo", &foo));
  ASSERT_EQ("bar", foo);

  // Bad types.
  ASSERT_TRUE(r.extractBool(r.root(), "foo", nullptr).IsInvalidArgument());
  ASSERT_TRUE(r.extractInt32(r.root(), "foo", nullptr).IsInvalidArgument());
  ASSERT_TRUE(r.extractInt64(r.root(), "foo", nullptr).IsInvalidArgument());
  ASSERT_TRUE(r.extractObject(r.root(), "foo", nullptr).IsInvalidArgument());
  ASSERT_TRUE(
      r.extractObjectArray(r.root(), "foo", nullptr).IsInvalidArgument());
}

TEST(JsonReaderTest, LessBasic) {
  string doc = fmt::format(
      "{{ \"small\" : 1, \"big\" : {}, \"null\" : null, \"empty\" : \"\", \"bool\" : true }}",
      std::numeric_limits<int64_t>::max());
  JsonReader r(doc);
  ASSERT_OK(r.init());
  int32_t small;
  ASSERT_OK(r.extractInt32(r.root(), "small", &small));
  ASSERT_EQ(1, small);
  int64_t big;
  ASSERT_OK(r.extractInt64(r.root(), "big", &big));
  ASSERT_EQ(std::numeric_limits<int64_t>::max(), big);
  string str;
  ASSERT_OK(r.extractString(r.root(), "null", &str));
  ASSERT_EQ("", str);
  ASSERT_OK(r.extractString(r.root(), "empty", &str));
  ASSERT_EQ("", str);
  bool b;
  ASSERT_OK(r.extractBool(r.root(), "bool", &b));
  ASSERT_TRUE(b);

  // Bad types.
  ASSERT_TRUE(r.extractBool(r.root(), "small", nullptr).IsInvalidArgument());
  ASSERT_TRUE(r.extractString(r.root(), "small", nullptr).IsInvalidArgument());
  ASSERT_TRUE(r.extractObject(r.root(), "small", nullptr).IsInvalidArgument());
  ASSERT_TRUE(
      r.extractObjectArray(r.root(), "small", nullptr).IsInvalidArgument());

  ASSERT_TRUE(r.extractBool(r.root(), "big", nullptr).IsInvalidArgument());
  ASSERT_TRUE(r.extractInt32(r.root(), "big", nullptr).IsInvalidArgument());
  ASSERT_TRUE(r.extractString(r.root(), "big", nullptr).IsInvalidArgument());
  ASSERT_TRUE(r.extractObject(r.root(), "big", nullptr).IsInvalidArgument());
  ASSERT_TRUE(
      r.extractObjectArray(r.root(), "big", nullptr).IsInvalidArgument());

  ASSERT_TRUE(r.extractBool(r.root(), "null", nullptr).IsInvalidArgument());
  ASSERT_TRUE(r.extractInt32(r.root(), "null", nullptr).IsInvalidArgument());
  ASSERT_TRUE(r.extractInt64(r.root(), "null", nullptr).IsInvalidArgument());
  ASSERT_TRUE(r.extractObject(r.root(), "null", nullptr).IsInvalidArgument());
  ASSERT_TRUE(
      r.extractObjectArray(r.root(), "null", nullptr).IsInvalidArgument());

  ASSERT_TRUE(r.extractBool(r.root(), "empty", nullptr).IsInvalidArgument());
  ASSERT_TRUE(r.extractInt32(r.root(), "empty", nullptr).IsInvalidArgument());
  ASSERT_TRUE(r.extractInt64(r.root(), "empty", nullptr).IsInvalidArgument());
  ASSERT_TRUE(r.extractObject(r.root(), "empty", nullptr).IsInvalidArgument());
  ASSERT_TRUE(
      r.extractObjectArray(r.root(), "empty", nullptr).IsInvalidArgument());

  ASSERT_TRUE(r.extractInt32(r.root(), "bool", nullptr).IsInvalidArgument());
  ASSERT_TRUE(r.extractInt64(r.root(), "bool", nullptr).IsInvalidArgument());
  ASSERT_TRUE(r.extractString(r.root(), "bool", nullptr).IsInvalidArgument());
  ASSERT_TRUE(r.extractObject(r.root(), "bool", nullptr).IsInvalidArgument());
  ASSERT_TRUE(
      r.extractObjectArray(r.root(), "bool", nullptr).IsInvalidArgument());
}

TEST(JsonReaderTest, Objects) {
  JsonReader r("{ \"foo\" : { \"1\" : 1 } }");
  ASSERT_OK(r.init());

  const Value* foo = nullptr;
  ASSERT_OK(r.extractObject(r.root(), "foo", &foo));
  ASSERT_TRUE(foo);

  int32_t one;
  ASSERT_OK(r.extractInt32(foo, "1", &one));
  ASSERT_EQ(1, one);

  // Bad types.
  ASSERT_TRUE(r.extractBool(r.root(), "foo", nullptr).IsInvalidArgument());
  ASSERT_TRUE(r.extractInt32(r.root(), "foo", nullptr).IsInvalidArgument());
  ASSERT_TRUE(r.extractInt64(r.root(), "foo", nullptr).IsInvalidArgument());
  ASSERT_TRUE(r.extractString(r.root(), "foo", nullptr).IsInvalidArgument());
  ASSERT_TRUE(
      r.extractObjectArray(r.root(), "foo", nullptr).IsInvalidArgument());
}

TEST(JsonReaderTest, TopLevelArray) {
  JsonReader r("[ { \"name\" : \"foo\" }, { \"name\" : \"bar\" } ]");
  ASSERT_OK(r.init());

  vector<const Value*> objs;
  ASSERT_OK(r.extractObjectArray(r.root(), nullptr, &objs));
  ASSERT_EQ(2, objs.size());
  string name;
  ASSERT_OK(r.extractString(objs[0], "name", &name));
  ASSERT_EQ("foo", name);
  ASSERT_OK(r.extractString(objs[1], "name", &name));
  ASSERT_EQ("bar", name);

  // Bad types.
  ASSERT_TRUE(r.extractBool(r.root(), nullptr, nullptr).IsInvalidArgument());
  ASSERT_TRUE(r.extractInt32(r.root(), nullptr, nullptr).IsInvalidArgument());
  ASSERT_TRUE(r.extractInt64(r.root(), nullptr, nullptr).IsInvalidArgument());
  ASSERT_TRUE(r.extractString(r.root(), nullptr, nullptr).IsInvalidArgument());
  ASSERT_TRUE(r.extractObject(r.root(), nullptr, nullptr).IsInvalidArgument());
}

TEST(JsonReaderTest, NestedArray) {
  JsonReader r(
      "{ \"foo\" : [ { \"val\" : 0 }, { \"val\" : 1 }, { \"val\" : 2 } ] }");
  ASSERT_OK(r.init());

  vector<const Value*> foo;
  ASSERT_OK(r.extractObjectArray(r.root(), "foo", &foo));
  ASSERT_EQ(3, foo.size());
  int i = 0;
  for (const Value* v : foo) {
    int32_t number;
    ASSERT_OK(r.extractInt32(v, "val", &number));
    ASSERT_EQ(i, number);
    i++;
  }

  // Bad types.
  ASSERT_TRUE(r.extractBool(r.root(), "foo", nullptr).IsInvalidArgument());
  ASSERT_TRUE(r.extractInt32(r.root(), "foo", nullptr).IsInvalidArgument());
  ASSERT_TRUE(r.extractInt64(r.root(), "foo", nullptr).IsInvalidArgument());
  ASSERT_TRUE(r.extractString(r.root(), "foo", nullptr).IsInvalidArgument());
  ASSERT_TRUE(r.extractObject(r.root(), "foo", nullptr).IsInvalidArgument());
}

} // namespace kudu
