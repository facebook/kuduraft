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

#include <string>

#include <gtest/gtest.h>
#include <rapidjson/document.h>
#include <rapidjson/rapidjson.h>

#include <cstdint>

#include "kudu/util/easy_json.h"
#include "kudu/util/test_util.h"

using rapidjson::SizeType;
using rapidjson::Value;
using std::string;

namespace kudu {

class EasyJsonTest : public KuduTest {};

TEST_F(EasyJsonTest, TestNull) {
  EasyJson ej;
  ASSERT_TRUE(ej.value().IsNull());
}

TEST_F(EasyJsonTest, TestBasic) {
  EasyJson ej;
  ej.setObject();
  ej.set("1", true);
  ej.set("2", std::numeric_limits<int32_t>::min());
  ej.set("4", std::numeric_limits<int64_t>::min());
  ej.set("6", 1.0);
  ej.set("7", "string");

  Value& v = ej.value();

  ASSERT_EQ(v["1"].GetBool(), true);
  ASSERT_EQ(v["2"].GetInt(), std::numeric_limits<int32_t>::min());
  ASSERT_EQ(v["4"].GetInt64(), std::numeric_limits<int64_t>::min());
  ASSERT_EQ(v["6"].GetDouble(), 1.0);
  ASSERT_EQ(string(v["7"].GetString()), "string");
}

TEST_F(EasyJsonTest, TestNested) {
  EasyJson ej;
  ej.setObject();
  ej.get("nested").setObject();
  ej.get("nested").set("nested_attr", true);
  ASSERT_EQ(ej.value()["nested"]["nested_attr"].GetBool(), true);

  ej.get("nested_array").setArray();
  ej.get("nested_array").pushBack(1);
  ej.get("nested_array").pushBack(2);
  ASSERT_EQ(ej.value()["nested_array"][SizeType(0)].GetInt(), 1);
  ASSERT_EQ(ej.value()["nested_array"][SizeType(1)].GetInt(), 2);
}

TEST_F(EasyJsonTest, TestCompactSyntax) {
  EasyJson ej;
  ej["nested"]["nested_attr"] = true;
  ASSERT_EQ(ej.value()["nested"]["nested_attr"].GetBool(), true);

  for (int i = 0; i < 2; i++) {
    ej["nested_array"][i] = i + 1;
  }
  ASSERT_EQ(ej.value()["nested_array"][SizeType(0)].GetInt(), 1);
  ASSERT_EQ(ej.value()["nested_array"][SizeType(1)].GetInt(), 2);
}

TEST_F(EasyJsonTest, TestComplexInitializer) {
  EasyJson ej;
  ej = EasyJson::kObject;
  ASSERT_TRUE(ej.value().IsObject());

  EasyJson nestedArr = ej.set("nested_arr", EasyJson::kArray);
  ASSERT_TRUE(nestedArr.value().IsArray());

  EasyJson nestedObj = nestedArr.pushBack(EasyJson::kObject);
  ASSERT_TRUE(ej["nested_arr"][0].value().IsObject());
}

TEST_F(EasyJsonTest, TestAllocatorLifetime) {
  EasyJson* root = new EasyJson;
  EasyJson child = (*root)["child"];
  delete root;

  child["child_attr"] = 1;
  ASSERT_EQ(child.value()["child_attr"].GetInt(), 1);
}

} // namespace kudu
