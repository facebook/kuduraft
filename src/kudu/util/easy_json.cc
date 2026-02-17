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

#include "kudu/util/easy_json.h"

#include <ostream>
#include <string>
#include <utility>

#include <glog/logging.h>
#include <rapidjson/document.h>
#include <rapidjson/rapidjson.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

using rapidjson::SizeType;
using rapidjson::Value;
using std::string;

namespace kudu {

EasyJson::EasyJson()
    : alloc_(std::make_shared<EasyJsonAllocator>()), value_(&alloc_->value()) {}

EasyJson::EasyJson(EasyJson::ComplexTypeInitializer type)
    : alloc_(std::make_shared<EasyJsonAllocator>()), value_(&alloc_->value()) {
  if (type == kObject) {
    value_->SetObject();
  } else if (type == kArray) {
    value_->SetArray();
  }
}

EasyJson EasyJson::get(const string& key) {
  if (!value_->IsObject()) {
    value_->SetObject();
  }
  if (!value_->HasMember(key.c_str())) {
    Value keyVal(key.c_str(), alloc_->allocator());
    value_->AddMember(keyVal, Value().SetNull(), alloc_->allocator());
  }
  return EasyJson(&(*value_)[key.c_str()], alloc_);
}

EasyJson EasyJson::get(int index) {
  if (!value_->IsArray()) {
    value_->SetArray();
  }
  while (SizeType(index) >= value_->Size()) {
    value_->PushBack(Value().SetNull(), alloc_->allocator());
  }
  return EasyJson(&(*value_)[index], alloc_);
}

EasyJson EasyJson::operator[](const string& key) {
  return get(key);
}

EasyJson EasyJson::operator[](int index) {
  return get(index);
}

EasyJson& EasyJson::operator=(const string& val) {
  value_->SetString(val.c_str(), alloc_->allocator());
  return *this;
}
template <typename T>
EasyJson& EasyJson::operator=(T val) {
  *value_ = val;
  return *this;
}
template EasyJson& EasyJson::operator= <bool>(bool val);
template EasyJson& EasyJson::operator= <int32_t>(int32_t val);
template EasyJson& EasyJson::operator= <int64_t>(int64_t val);
template EasyJson& EasyJson::operator= <uint32_t>(uint32_t val);
template EasyJson& EasyJson::operator= <uint64_t>(uint64_t val);
template EasyJson& EasyJson::operator= <double>(double val);
template <>
EasyJson& EasyJson::operator= <const char*>(const char* val) {
  value_->SetString(val, alloc_->allocator());
  return *this;
}
template <>
EasyJson& EasyJson::operator=
    <EasyJson::ComplexTypeInitializer>(EasyJson::ComplexTypeInitializer val) {
  if (val == kObject) {
    value_->SetObject();
  } else if (val == kArray) {
    value_->SetArray();
  }
  return (*this);
}

EasyJson& EasyJson::setObject() {
  if (!value_->IsObject()) {
    value_->SetObject();
  }
  return *this;
}

EasyJson& EasyJson::setArray() {
  if (!value_->IsArray()) {
    value_->SetArray();
  }
  return *this;
}

EasyJson EasyJson::set(const string& key, const string& val) {
  return (get(key) = val);
}
template <typename T>
EasyJson EasyJson::set(const string& key, T val) {
  return (get(key) = val);
}
template EasyJson EasyJson::set<bool>(const string& key, bool val);
template EasyJson EasyJson::set<int32_t>(const string& key, int32_t val);
template EasyJson EasyJson::set<int64_t>(const string& key, int64_t val);
template EasyJson EasyJson::set<uint32_t>(const string& key, uint32_t val);
template EasyJson EasyJson::set<uint64_t>(const string& key, uint64_t val);
template EasyJson EasyJson::set<double>(const string& key, double val);
template EasyJson EasyJson::set<const char*>(
    const string& key,
    const char* val);
template EasyJson EasyJson::set<EasyJson::ComplexTypeInitializer>(
    const string& key,
    EasyJson::ComplexTypeInitializer val);

EasyJson EasyJson::set(int index, const string& val) {
  return (get(index) = val);
}
template <typename T>
EasyJson EasyJson::set(int index, T val) {
  return (get(index) = val);
}
template EasyJson EasyJson::set<bool>(int index, bool val);
template EasyJson EasyJson::set<int32_t>(int index, int32_t val);
template EasyJson EasyJson::set<int64_t>(int index, int64_t val);
template EasyJson EasyJson::set<uint32_t>(int index, uint32_t val);
template EasyJson EasyJson::set<uint64_t>(int index, uint64_t val);
template EasyJson EasyJson::set<double>(int index, double val);
template EasyJson EasyJson::set<const char*>(int index, const char* val);
template EasyJson EasyJson::set<EasyJson::ComplexTypeInitializer>(
    int index,
    EasyJson::ComplexTypeInitializer val);

EasyJson EasyJson::pushBack(const string& val) {
  if (!value_->IsArray()) {
    value_->SetArray();
  }
  Value pushVal(val.c_str(), alloc_->allocator());
  value_->PushBack(pushVal, alloc_->allocator());
  return EasyJson(&(*value_)[value_->Size() - 1], alloc_);
}
template <typename T>
EasyJson EasyJson::pushBack(T val) {
  if (!value_->IsArray()) {
    value_->SetArray();
  }
  value_->PushBack(val, alloc_->allocator());
  return EasyJson(&(*value_)[value_->Size() - 1], alloc_);
}
template EasyJson EasyJson::pushBack<bool>(bool val);
template EasyJson EasyJson::pushBack<int32_t>(int32_t val);
template EasyJson EasyJson::pushBack<int64_t>(int64_t val);
template EasyJson EasyJson::pushBack<uint32_t>(uint32_t val);
template EasyJson EasyJson::pushBack<uint64_t>(uint64_t val);
template EasyJson EasyJson::pushBack<double>(double val);
template <>
EasyJson EasyJson::pushBack<const char*>(const char* val) {
  if (!value_->IsArray()) {
    value_->SetArray();
  }
  Value pushVal(val, alloc_->allocator());
  value_->PushBack(pushVal, alloc_->allocator());
  return EasyJson(&(*value_)[value_->Size() - 1], alloc_);
}
template <>
EasyJson EasyJson::pushBack<EasyJson::ComplexTypeInitializer>(
    EasyJson::ComplexTypeInitializer val) {
  if (!value_->IsArray()) {
    value_->SetArray();
  }
  Value pushVal;
  if (val == kObject) {
    pushVal.SetObject();
  } else if (val == kArray) {
    pushVal.SetArray();
  } else {
    LOG(FATAL) << "Unknown initializer type";
  }
  value_->PushBack(pushVal, alloc_->allocator());
  return EasyJson(&(*value_)[value_->Size() - 1], alloc_);
}

string EasyJson::toString() const {
  rapidjson::StringBuffer buffer;
  rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
  value_->Accept(writer);
  return buffer.GetString();
}

EasyJson::EasyJson(Value* value, std::shared_ptr<EasyJsonAllocator> alloc)
    : alloc_(std::move(alloc)), value_(value) {}

} // namespace kudu
