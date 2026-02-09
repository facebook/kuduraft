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

#include "kudu/util/flag_tags.h"

#include <map>
#include <ostream>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include <glog/logging.h>

#include "kudu/gutil/singleton.h"

using std::multimap;
using std::pair;
using std::string;
using std::unordered_set;

namespace kudu {
namespace flag_tags_internal {

// Singleton registry storing the set of tags for each flag.
class FlagTagRegistry {
 public:
  static FlagTagRegistry* getInstance() {
    return Singleton<FlagTagRegistry>::get();
  }

  void addTag(const string& name, const string& tag) {
    tagMap_.insert(TagMap::value_type(name, tag));
  }

  void getTags(const string& name, unordered_set<string>* tags) {
    tags->clear();
    pair<TagMap::const_iterator, TagMap::const_iterator> range =
        tagMap_.equal_range(name);
    for (auto it = range.first; it != range.second; ++it) {
      if (!tags->insert(it->second).second) {
        LOG(DFATAL) << "Flag " << name
                    << " was tagged more than once with the tag '" << it->second
                    << "'";
      }
    }
  }

 private:
  friend class Singleton<FlagTagRegistry>;
  FlagTagRegistry() {}

  using TagMap = multimap<string, string>;
  TagMap tagMap_;

  DISALLOW_COPY_AND_ASSIGN(FlagTagRegistry);
};

FlagTagger::FlagTagger(const char* name, const char* tag) {
  FlagTagRegistry::getInstance()->addTag(name, tag);
}

FlagTagger::~FlagTagger() {}

} // namespace flag_tags_internal

using flag_tags_internal::FlagTagRegistry;

void getFlagTags(const string& flagName, unordered_set<string>* tags) {
  FlagTagRegistry::getInstance()->getTags(flagName, tags);
}

} // namespace kudu
