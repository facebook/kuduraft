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

// All rights reserved.

#include <algorithm>
#include <cstdlib>
#include <map>
#include <memory>
#include <ostream>
#include <string>
#include <tuple> // IWYU pragma: keep
#include <utility>
#include <vector>

#include <glog/logging.h>
#include <gtest/gtest.h>
#include <optional>

#include <fmt/core.h>
#include "kudu/util/interval_tree-inl.h"
#include "kudu/util/interval_tree.h"
#include "kudu/util/test_util.h"

using std::pair;
using std::string;
using std::vector;

namespace kudu {

// Test harness.
class TestIntervalTree : public KuduTest {};

// Simple interval class for integer intervals.
struct IntInterval {
  IntInterval(int left, int right, int id = -1)
      : left(left), right(right), id(id) {}

  // {} means infinity.
  // [left,  right] is closed interval.
  // [lower, upper) is half-open interval, so the upper is exclusive.
  bool intersects(
      const std::optional<int>& lower,
      const std::optional<int>& upper) const {
    if (!lower && !upper) {
      //         [left, right]
      //            |     |
      // [-OO,                      +OO)
    } else if (!lower) {
      //         [left, right]
      //            |
      // [-OO,    upper)
      if (*upper <= this->left) {
        return false;
      }
    } else if (!upper) {
      //         [left, right]
      //                     \
      //                      [lower, +OO)
      if (*lower > this->right) {
        return false;
      }
    } else {
      //         [left, right]
      //                     \
      //                      [lower, upper)
      if (*lower > this->right) {
        return false;
      }
      //         [left, right]
      //            |
      // [lower,  upper)
      if (*upper <= this->left) {
        return false;
      }
    }
    return true;
  }

  string toString() const {
    return fmt::format("[{}, {}]({}) ", left, right, id);
  }

  int left, right, id;
};

// A wrapper around an int which can be compared with IntTraits::compare()
// but also can keep a counter of how many times it has been compared. Used
// for TestBigO below.
struct CountingQueryPoint {
  explicit CountingQueryPoint(int v) : val(v), count(new int(0)) {}

  int val;
  std::shared_ptr<int> count;
};

// Traits definition for intervals made up of ints on either end.
struct IntTraits {
  using PointType = int;
  using IntervalType = IntInterval;
  static PointType getLeft(const IntInterval& x) {
    return x.left;
  }
  static PointType getRight(const IntInterval& x) {
    return x.right;
  }
  static int compare(int a, int b) {
    if (a < b) {
      return -1;
    }
    if (a > b) {
      return 1;
    }
    return 0;
  }

  static int compare(const CountingQueryPoint& q, int b) {
    (*q.count)++;
    return compare(q.val, b);
  }
  static int compare(int a, const CountingQueryPoint& b) {
    return -compare(b, a);
  }

  static int compare(
      const std::optional<int>& a,
      const int b,
      const EndpointIfNone& type) {
    if (!a) {
      return ((kPositiveInfinity == type) ? 1 : -1);
    }

    return compare(*a, b);
  }

  static int compare(
      const int a,
      const std::optional<int>& b,
      const EndpointIfNone& type) {
    return -compare(b, a, type);
  }
};

// Compare intervals in an arbitrary but consistent way - this is only
// used for verifying that the two algorithms come up with the same results.
// It's not necessary to define this to use an interval tree.
static bool compareIntervals(const IntInterval& a, const IntInterval& b) {
  return std::make_tuple(a.left, a.right, a.id) <
      std::make_tuple(b.left, b.right, b.id);
}

// Stringify a list of int intervals, for easy test error reporting.
static string stringify(const vector<IntInterval>& intervals) {
  string ret;
  bool first = true;
  for (const IntInterval& interval : intervals) {
    if (!first) {
      ret.append(",");
    }
    ret.append(interval.toString());
  }
  return ret;
}

// Find any intervals in 'intervals' which contain 'queryPoint' by brute force.
static void findContainingBruteForce(
    const vector<IntInterval>& intervals,
    int queryPoint,
    vector<IntInterval>* results) {
  for (const IntInterval& i : intervals) {
    if (queryPoint >= i.left && queryPoint <= i.right) {
      results->push_back(i);
    }
  }
}

// Find any intervals in 'intervals' which intersect 'query_interval' by brute
// force.
static void findIntersectingBruteForce(
    const vector<IntInterval>& intervals,
    const std::optional<int>& lower,
    const std::optional<int>& upper,
    vector<IntInterval>* results) {
  for (const IntInterval& i : intervals) {
    if (i.intersects(lower, upper)) {
      results->push_back(i);
    }
  }
}

// Verify that IntervalTree::findContainingPoint yields the same results as the
// naive brute-force O(n) algorithm.
static void verifyFindContainingPoint(
    const vector<IntInterval>& allIntervals,
    const IntervalTree<IntTraits>& tree,
    int queryPoint) {
  vector<IntInterval> results;
  tree.findContainingPoint(queryPoint, &results);
  std::sort(results.begin(), results.end(), compareIntervals);

  vector<IntInterval> bruteForce;
  findContainingBruteForce(allIntervals, queryPoint, &bruteForce);
  std::sort(bruteForce.begin(), bruteForce.end(), compareIntervals);

  SCOPED_TRACE(stringify(allIntervals) + fmt::format(" {{q={}}}", queryPoint));
  EXPECT_EQ(stringify(bruteForce), stringify(results));
}

// Verify that IntervalTree::findIntersectingInterval yields the same results as
// the naive brute-force O(n) algorithm.
static void verifyFindIntersectingInterval(
    const vector<IntInterval>& allIntervals,
    const IntervalTree<IntTraits>& tree,
    const IntInterval& queryInterval) {
  const auto& process = [&](const std::optional<int>& lower,
                            const std::optional<int>& upper) {
    vector<IntInterval> results;
    tree.findIntersectingInterval(lower, upper, &results);
    std::sort(results.begin(), results.end(), compareIntervals);

    vector<IntInterval> bruteForce;
    findIntersectingBruteForce(allIntervals, lower, upper, &bruteForce);
    std::sort(bruteForce.begin(), bruteForce.end(), compareIntervals);
    EXPECT_EQ(stringify(bruteForce), stringify(results));
  };

  {
    // [lower, upper)
    std::optional<int> lower = queryInterval.left;
    std::optional<int> upper = queryInterval.right;
    SCOPED_TRACE(
        stringify(allIntervals) +
        fmt::format(" {{q=[{}, {})}}", *lower, *upper));
    process(lower, upper);
  }

  {
    // [-OO, upper)
    std::optional<int> lower = {};
    std::optional<int> upper = queryInterval.right;
    SCOPED_TRACE(
        stringify(allIntervals) + fmt::format(" {{q=[-OO, {})}}", *upper));
    process(lower, upper);
  }

  {
    // [lower, +OO)
    std::optional<int> lower = queryInterval.left;
    std::optional<int> upper = {};
    SCOPED_TRACE(
        stringify(allIntervals) + fmt::format(" {{q=[{}, +OO)}}", *lower));
    process(lower, upper);
  }

  {
    // [-OO, +OO)
    std::optional<int> lower = queryInterval.left;
    std::optional<int> upper = {};
    SCOPED_TRACE(stringify(allIntervals) + fmt::format(" {{q=[-OO, +OO)}}"));
    process(lower, upper);
  }
}

static vector<IntInterval> createRandomIntervals(int n = 100) {
  vector<IntInterval> intervals;
  for (int i = 0; i < n; i++) {
    int l = rand() % 100; // NOLINT(runtime/threadsafe_fn)
    int r = l + rand() % 20; // NOLINT(runtime/threadsafe_fn)
    intervals.emplace_back(l, r, i);
  }
  return intervals;
}

TEST_F(TestIntervalTree, TestBasic) {
  vector<IntInterval> intervals;
  intervals.emplace_back(1, 2, 1);
  intervals.emplace_back(3, 4, 2);
  intervals.emplace_back(1, 4, 3);
  IntervalTree<IntTraits> t(intervals);

  for (int i = 0; i <= 5; i++) {
    verifyFindContainingPoint(intervals, t, i);

    for (int j = i; j <= 5; j++) {
      verifyFindIntersectingInterval(intervals, t, IntInterval(i, j, 0));
    }
  }
}

TEST_F(TestIntervalTree, TestRandomized) {
  SeedRandom();

  // Generate 100 random intervals spanning 0-200 and build an interval tree
  // from them.
  vector<IntInterval> intervals = createRandomIntervals();
  IntervalTree<IntTraits> t(intervals);

  // Test that we get the correct result on every possible query.
  for (int i = -1; i < 201; i++) {
    verifyFindContainingPoint(intervals, t, i);
  }

  // Test that we get the correct result for random intervals
  for (int i = 0; i < 100; i++) {
    int l = rand() % 100; // NOLINT(runtime/threadsafe_fn)
    int r = l + rand() % 100; // NOLINT(runtime/threadsafe_fn)
    verifyFindIntersectingInterval(intervals, t, IntInterval(l, r));
  }
}

TEST_F(TestIntervalTree, TestEmpty) {
  vector<IntInterval> empty;
  IntervalTree<IntTraits> t(empty);

  verifyFindContainingPoint(empty, t, 1);
  verifyFindIntersectingInterval(empty, t, IntInterval(1, 2, 0));
}

TEST_F(TestIntervalTree, TestBigO) {
#ifndef NDEBUG
  LOG(WARNING) << "big-O results are not valid if DCHECK is enabled";
  return;
#endif
  SeedRandom();

  LOG(INFO) << "num_int\tnum_q\tresults\tsimple\tbatch";
  for (int numIntervals = 1; numIntervals < 2000; numIntervals *= 2) {
    vector<IntInterval> intervals = createRandomIntervals(numIntervals);
    IntervalTree<IntTraits> t(intervals);
    for (int numQueries = 1; numQueries < 2000; numQueries *= 2) {
      vector<CountingQueryPoint> queries;
      for (int i = 0; i < numQueries; i++) {
        queries.emplace_back(rand() % 100);
      }
      std::sort(
          queries.begin(),
          queries.end(),
          [](const CountingQueryPoint& a, const CountingQueryPoint& b) {
            return a.val < b.val;
          });

      // Test using batch algorithm.
      int numResultsBatch = 0;
      t.forEachIntervalContainingPoints(
          queries,
          [&](CountingQueryPoint queryPoint, const IntInterval& interval) {
            numResultsBatch++;
          });
      int numComparisonsBatch = 0;
      for (const auto& q : queries) {
        numComparisonsBatch += *q.count;
        *q.count = 0;
      }

      // Test using one-by-one queries.
      int numResultsSimple = 0;
      for (auto& q : queries) {
        vector<IntInterval> results;
        t.findContainingPoint(q, &results);
        numResultsSimple += results.size();
      }
      int numComparisonsSimple = 0;
      for (const auto& q : queries) {
        numComparisonsSimple += *q.count;
      }
      ASSERT_EQ(numResultsSimple, numResultsBatch);

      LOG(INFO) << numIntervals << "\t" << numQueries << "\t"
                << numResultsSimple << "\t" << numComparisonsSimple << "\t"
                << numComparisonsBatch;
    }
  }
}

TEST_F(TestIntervalTree, TestMultiQuery) {
  SeedRandom();
  const int kNumQueries = 1;
  vector<IntInterval> intervals = createRandomIntervals(10);
  IntervalTree<IntTraits> t(intervals);

  // Generate random queries.
  vector<int> queries;
  for (int i = 0; i < kNumQueries; i++) {
    queries.push_back(rand() % 100);
  }
  std::sort(queries.begin(), queries.end());

  vector<pair<string, int>> resultsSimple;
  for (int q : queries) {
    vector<IntInterval> results;
    t.findContainingPoint(q, &results);
    for (const auto& interval : results) {
      resultsSimple.emplace_back(interval.toString(), q);
    }
  }

  vector<pair<string, int>> resultsBatch;
  t.forEachIntervalContainingPoints(
      queries, [&](int queryPoint, const IntInterval& interval) {
        resultsBatch.emplace_back(interval.toString(), queryPoint);
      });

  // Check the property that, when the batch query points are in sorted order,
  // the results are grouped by interval, and within each interval, sorted by
  // query point. Each interval may have at most two groups.
  std::optional<pair<string, int>> prev = {};
  std::map<string, int> intervalsSeen;
  for (int i = 0; i < resultsBatch.size(); i++) {
    const auto& cur = resultsBatch[i];
    // If it's another query point hitting the same interval,
    // make sure the query points are returned in order.
    if (prev && prev->first == cur.first) {
      EXPECT_GE(cur.second, prev->second) << prev->first;
    } else {
      // It's the start of a new interval's data. Make sure that we don't
      // see the same interval twice.
      EXPECT_LE(++intervalsSeen[cur.first], 2)
          << "Saw more than two groups for interval " << cur.first;
    }
    prev = cur;
  }

  std::sort(resultsSimple.begin(), resultsSimple.end());
  std::sort(resultsBatch.begin(), resultsBatch.end());
  ASSERT_EQ(resultsSimple, resultsBatch);
}

} // namespace kudu
