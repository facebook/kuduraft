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
#ifndef KUDU_UTIL_KNAPSACK_SOLVER_H
#define KUDU_UTIL_KNAPSACK_SOLVER_H

#include <glog/logging.h>
#include <algorithm>
#include <utility>
#include <vector>
#include "kudu/gutil/macros.h"

namespace kudu {

// Solver for the 0-1 knapsack problem. This uses dynamic programming
// to solve the problem exactly.
//
// Given a knapsack capacity of 'W' and a number of potential items 'n',
// this solver is O(nW) time and space.
//
// This implementation is cribbed from wikipedia. The only interesting
// bit here that doesn't directly match the pseudo-code is that we
// maintain the "taken" bitmap keeping track of which items were
// taken, so we can efficiently "trace back" the chosen items.
template <class Traits>
class KnapsackSolver {
 public:
  using ItemType = typename Traits::ItemType;
  using ValueType = typename Traits::ValueType;
  using SolutionType = std::pair<int, ValueType>;

  KnapsackSolver() {}
  ~KnapsackSolver() {}

  // Solve a knapsack problem in one shot. Finds the set of
  // items in 'items' such that their weights add up to no
  // more than 'knapsack_capacity' and maximizes the sum
  // of their values.
  // The indexes of the chosen items are stored in 'chosen_items',
  // and the maximal value is stored in 'optimal_value'.
  void solve(
      std::vector<ItemType>& items,
      int knapsackCapacity,
      std::vector<int>* chosenItems,
      ValueType* optimalValue);

  // The following functions are a more advanced API for solving
  // knapsack problems, allowing the caller to obtain incremental
  // results as each item is considered. See the implementation of
  // Solve() for usage.

  // Prepare to solve a knapsack problem with the given capacity and
  // item set. The vector of items must remain valid and unchanged
  // until the next call to reset().
  void reset(int knapsackCapacity, const std::vector<ItemType>* items);

  // Process the next item in 'items'. Returns false if there
  // were no more items to process.
  bool processNext();

  // Returns the current best solution after the most recent processNext
  // call. *solution is a pair of (knapsack weight used, value obtained).
  SolutionType getSolution();

  // Trace the path of item indexes used to achieve the given best
  // solution as of the latest processNext() call.
  void tracePath(const SolutionType& best, std::vector<int>* chosenItems);

 private:
  // The state kept by the DP algorithm.
  class KnapsackBlackboard {
   public:
    using SolutionType = std::pair<int, ValueType>;
    KnapsackBlackboard()
        : nItems_(0), nWeights_(0), curItemIdx_(0), bestSolution_(0, 0) {}

    void resizeAndClear(int nItems, int maxWeight);

    // Current maximum value at the given weight
    ValueType& maxAt(int weight) {
      DCHECK_GE(weight, 0);
      DCHECK_LT(weight, nWeights_);
      return maxValue_[weight];
    }

    // Consider the next item to be put into the knapsack
    // Moves the "state" of the solution forward
    void advance(ValueType newVal, int newWt);

    // How many items have been considered
    int currentItemIndex() const {
      return curItemIdx_;
    }

    bool itemTaken(int item, int weight) const {
      DCHECK_GE(weight, 0);
      DCHECK_LT(weight, nWeights_);
      DCHECK_GE(item, 0);
      DCHECK_LT(item, nItems_);
      return itemTaken_[index(item, weight)];
    }

    SolutionType bestSolution() {
      return bestSolution_;
    }

    bool done() {
      return curItemIdx_ == nItems_;
    }

   private:
    void markTaken(int item, int weight) {
      itemTaken_[index(item, weight)] = true;
    }

    // If the dynamic programming matrix has more than this number of cells,
    // then warn.
    static const int kWarnDimension = 10000000;

    int index(int item, int weight) const {
      return nWeights_ * item + weight;
    }

    // vector with maximum value at the i-th position meaning that it is
    // the maximum value you can get given a knapsack of weight capacity i
    // while only considering items 0..curItemIdx_-1
    std::vector<ValueType> maxValue_;
    std::vector<bool> itemTaken_; // TODO: record difference vectors?
    int nItems_, nWeights_;
    int curItemIdx_;
    // Best current solution
    SolutionType bestSolution_;

    DISALLOW_COPY_AND_ASSIGN(KnapsackBlackboard);
  };

  KnapsackBlackboard bb_;
  const std::vector<ItemType>* items_;
  int knapsackCapacity_;

  DISALLOW_COPY_AND_ASSIGN(KnapsackSolver);
};

template <class Traits>
inline void KnapsackSolver<Traits>::reset(
    int knapsackCapacity,
    const std::vector<ItemType>* items) {
  DCHECK_GE(knapsackCapacity, 0);
  items_ = items;
  knapsackCapacity_ = knapsackCapacity;
  bb_.resizeAndClear(items->size(), knapsackCapacity);
}

template <class Traits>
inline bool KnapsackSolver<Traits>::processNext() {
  if (bb_.done()) {
    return false;
  }

  const ItemType& item = (*items_)[bb_.currentItemIndex()];
  int itemWeight = Traits::getWeight(item);
  ValueType itemValue = Traits::getValue(item);
  bb_.advance(itemValue, itemWeight);

  return true;
}

template <class Traits>
inline void KnapsackSolver<Traits>::solve(
    std::vector<ItemType>& items,
    int knapsackCapacity,
    std::vector<int>* chosenItems,
    ValueType* optimalValue) {
  reset(knapsackCapacity, &items);

  while (processNext()) {
  }

  SolutionType best = getSolution();
  *optimalValue = best.second;
  tracePath(best, chosenItems);
}

template <class Traits>
inline typename KnapsackSolver<Traits>::SolutionType
KnapsackSolver<Traits>::getSolution() {
  return bb_.bestSolution();
}

template <class Traits>
inline void KnapsackSolver<Traits>::tracePath(
    const SolutionType& best,
    std::vector<int>* chosenItems) {
  chosenItems->clear();
  // Retrace back which set of items corresponded to this value.
  int w = best.first;
  chosenItems->clear();
  for (int k = bb_.currentItemIndex() - 1; k >= 0; k--) {
    if (bb_.itemTaken(k, w)) {
      const ItemType& taken = (*items_)[k];
      chosenItems->push_back(k);
      w -= Traits::getWeight(taken);
      DCHECK_GE(w, 0);
    }
  }
}

template <class Traits>
void KnapsackSolver<Traits>::KnapsackBlackboard::resizeAndClear(
    int nItems,
    int maxWeight) {
  CHECK_GT(nItems, 0);
  CHECK_GE(maxWeight, 0);

  // Rather than zero-indexing the weights, we size the array from
  // 0 to maxWeight. This avoids having to subtract 1 every time
  // we index into the array.
  nWeights_ = maxWeight + 1;
  maxValue_.resize(nWeights_);

  int dimension = index(nItems, nWeights_);
  if (dimension > kWarnDimension) {
    LOG(WARNING) << "Knapsack problem " << nItems << "x" << nWeights_
                 << " is large: may be inefficient!";
  }
  itemTaken_.resize(dimension);
  nItems_ = nItems;

  // Clear
  std::fill(maxValue_.begin(), maxValue_.end(), 0);
  std::fill(itemTaken_.begin(), itemTaken_.end(), false);
  bestSolution_ = std::make_pair(0, 0);

  curItemIdx_ = 0;
}

template <class Traits>
void KnapsackSolver<Traits>::KnapsackBlackboard::advance(
    ValueType newVal,
    int newWt) {
  // Use the dynamic programming formula:
  // Define mv(i, j) as maximum value considering items 0..i-1 with knapsack
  // weight j Then: if j - weight(i) >= 0, then: mv(i, j) = max(mv(i-1, j),
  // mv(i-1, j-weight(i)) + value(j)) else mv(i, j) = mv(i-1, j) Since the
  // recursive formula requires an access of j-weight(i), we go in reverse.
  for (int j = nWeights_ - 1; j >= newWt; --j) {
    ValueType valIfTaken = maxValue_[j - newWt] + newVal;
    if (maxValue_[j] < valIfTaken) {
      maxValue_[j] = valIfTaken;
      markTaken(curItemIdx_, j);
      // Check if new solution found
      if (bestSolution_.second < valIfTaken) {
        bestSolution_ = std::make_pair(j, valIfTaken);
      }
    }
  }

  curItemIdx_++;
}

} // namespace kudu
#endif
