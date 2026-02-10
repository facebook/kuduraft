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

#include <cstdlib>
#include <memory>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "kudu/util/knapsack_solver.h"
#include "kudu/util/stopwatch.h" // IWYU pragma: keep
#include "kudu/util/test_util.h"

using std::string;
using std::vector;

namespace kudu {

class TestKnapsack : public KuduTest {};

// A simple test item for use with the knapsack solver.
// The real code will be solving knapsack over RowSet objects --
// using simple value/weight pairs in the tests makes it standalone.
struct TestItem {
  TestItem(double v, int w) : value(v), weight(w) {}

  double value;
  int weight;
};

// A traits class to adapt the knapsack solver to TestItem.
struct TestItemTraits {
  using item_type = TestItem;
  using value_type = double;
  static int getWeight(const TestItem& item) {
    return item.weight;
  }
  static value_type getValue(const TestItem& item) {
    return item.value;
  }
};

// Generate random items into the provided vector.
static void
generateRandomItems(int nItems, int maxWeight, vector<TestItem>* out) {
  for (int i = 0; i < nItems; i++) {
    double value = 10000.0 / (random() % 10000 + 1);
    int weight = random() % maxWeight;
    out->emplace_back(value, weight);
  }
}

// Join and stringify the given list of ints.
static string joinInts(const vector<int>& ints) {
  string ret;
  for (int i = 0; i < ints.size(); i++) {
    if (i > 0) {
      ret.push_back(',');
    }
    ret.append(std::to_string(ints[i]));
  }
  return ret;
}

TEST_F(TestKnapsack, Basics) {
  KnapsackSolver<TestItemTraits> solver;

  vector<TestItem> in;
  in.emplace_back(500, 3);
  in.emplace_back(110, 1);
  in.emplace_back(125, 1);
  in.emplace_back(100, 1);

  vector<int> out;
  double maxVal;

  // For 1 weight, pick item 2
  solver.solve(in, 1, &out, &maxVal);
  ASSERT_DOUBLE_EQ(125, maxVal);
  ASSERT_EQ("2", joinInts(out));
  out.clear();

  // For 2 weight, pick item 1, 2
  solver.solve(in, 2, &out, &maxVal);
  ASSERT_DOUBLE_EQ(110 + 125, maxVal);
  ASSERT_EQ("2,1", joinInts(out));
  out.clear();

  // For 3 weight, pick item 0
  solver.solve(in, 3, &out, &maxVal);
  ASSERT_DOUBLE_EQ(500, maxVal);
  ASSERT_EQ("0", joinInts(out));
  out.clear();

  // For 10 weight, pick all.
  solver.solve(in, 10, &out, &maxVal);
  ASSERT_DOUBLE_EQ(500 + 110 + 125 + 100, maxVal);
  ASSERT_EQ("3,2,1,0", joinInts(out));
  out.clear();
}

// Test which generates random knapsack instances and verifies
// that the result satisfies the constraints.
TEST_F(TestKnapsack, Randomized) {
  SeedRandom();
  KnapsackSolver<TestItemTraits> solver;

  const int kNumTrials = AllowSlowTests() ? 200 : 1;
  const int kMaxWeight = 1000;
  const int kNumItems = 1000;

  for (int i = 0; i < kNumTrials; i++) {
    vector<TestItem> in;
    vector<int> out;
    generateRandomItems(kNumItems, kMaxWeight, &in);
    double maxVal;
    int maxWeight = random() % kMaxWeight;
    solver.solve(in, maxWeight, &out, &maxVal);

    // Verify that the maxVal is equal to the sum of the chosen items' values.
    double sumVal = 0;
    int sumWeight = 0;
    for (int idx : out) {
      sumVal += in[idx].value;
      sumWeight += in[idx].weight;
    }
    ASSERT_NEAR(maxVal, sumVal, 0.000001);
    ASSERT_LE(sumWeight, maxWeight);
  }
}

#ifdef NDEBUG
TEST_F(TestKnapsack, Benchmark) {
  KnapsackSolver<TestItemTraits> solver;

  const int kNumTrials = 1000;
  const int kMaxWeight = 1000;
  const int kNumItems = 1000;

  vector<TestItem> in;
  generateRandomItems(kNumItems, kMaxWeight, &in);

  LOG_TIMING(INFO, "benchmark") {
    vector<int> out;
    for (int i = 0; i < kNumTrials; i++) {
      out.clear();
      double maxVal;
      solver.solve(in, random() % kMaxWeight, &out, &maxVal);
    }
  }
}
#endif

} // namespace kudu
