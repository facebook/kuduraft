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
//
// Portions of these classes were ported from Java to C++ from the sources
// available at https://github.com/HdrHistogram/HdrHistogram .
//
//   The code in this repository code was Written by Gil Tene, Michael Barker,
//   and Matt Warren, and released to the public domain, as explained at
//   http://creativecommons.org/publicdomain/zero/1.0/
#include "kudu/util/hdr_histogram.h"

#include <algorithm>
#include <cmath>
#include <limits>
#include <ostream>
#include <string>

#include <glog/logging.h>

#include <fmt/core.h>
#include "kudu/gutil/atomicops.h"
#include "kudu/gutil/bits.h"
#include "kudu/util/status.h"

using base::subtle::Atomic64;
using base::subtle::NoBarrier_AtomicIncrement;
using base::subtle::NoBarrier_CompareAndSwap;
using base::subtle::NoBarrier_Load;
using base::subtle::NoBarrier_Store;

namespace kudu {

const uint64_t HdrHistogram::kMinHighestTrackableValue;
const int HdrHistogram::kMinValidNumSignificantDigits;
const int HdrHistogram::kMaxValidNumSignificantDigits;

HdrHistogram::HdrHistogram(
    uint64_t highestTrackableValue,
    int numSignificantDigits)
    : highestTrackableValue_(highestTrackableValue),
      numSignificantDigits_(numSignificantDigits),
      countsArrayLength_(0),
      bucketCount_(0),
      subBucketCount_(0),
      subBucketHalfCountMagnitude_(0),
      subBucketHalfCount_(0),
      subBucketMask_(0),
      totalCount_(0),
      totalSum_(0),
      minValue_(std::numeric_limits<Atomic64>::max()),
      maxValue_(0) {
  init();
}

HdrHistogram::HdrHistogram(const HdrHistogram& other)
    : highestTrackableValue_(other.highestTrackableValue_),
      numSignificantDigits_(other.numSignificantDigits_),
      countsArrayLength_(0),
      bucketCount_(0),
      subBucketCount_(0),
      subBucketHalfCountMagnitude_(0),
      subBucketHalfCount_(0),
      subBucketMask_(0),
      totalCount_(0),
      totalSum_(0),
      minValue_(std::numeric_limits<Atomic64>::max()),
      maxValue_(0) {
  init();

  // Not a consistent snapshot but we try to roughly keep it close.
  // Copy the sum and min first.
  NoBarrier_Store(&totalSum_, NoBarrier_Load(&other.totalSum_));
  NoBarrier_Store(&minValue_, NoBarrier_Load(&other.minValue_));

  uint64_t total_copied_count = 0;
  // Copy the counts in order of ascending magnitude.
  for (int i = 0; i < countsArrayLength_; i++) {
    uint64_t count = NoBarrier_Load(&other.counts_[i]);
    NoBarrier_Store(&counts_[i], count);
    total_copied_count += count;
  }
  // Copy the max observed value last.
  NoBarrier_Store(&maxValue_, NoBarrier_Load(&other.maxValue_));
  // We must ensure the total is consistent with the copied counts.
  NoBarrier_Store(&totalCount_, total_copied_count);
}

bool HdrHistogram::isValidHighestTrackableValue(
    uint64_t highestTrackableValue) {
  return highestTrackableValue >= kMinHighestTrackableValue;
}

bool HdrHistogram::isValidNumSignificantDigits(int numSignificantDigits) {
  return numSignificantDigits >= kMinValidNumSignificantDigits &&
      numSignificantDigits <= kMaxValidNumSignificantDigits;
}

void HdrHistogram::init() {
  // Verify parameter validity
  CHECK(isValidHighestTrackableValue(highestTrackableValue_)) << fmt::format(
      "highestTrackableValue must be >= {}", kMinHighestTrackableValue);
  CHECK(isValidNumSignificantDigits(numSignificantDigits_)) << fmt::format(
      "numSignificantDigits must be between {} and {}",
      kMinValidNumSignificantDigits,
      kMaxValidNumSignificantDigits);

  uint32_t largest_value_with_single_unit_resolution =
      2 * static_cast<uint32_t>(pow(10.0, numSignificantDigits_));

  // We need to maintain power-of-two subBucketCount_ (for clean direct
  // indexing) that is large enough to provide unit resolution to at least
  // largest_value_with_single_unit_resolution. So figure out
  // largest_value_with_single_unit_resolution's nearest power-of-two
  // (rounded up), and use that:

  // The sub-buckets take care of the precision.
  // Each sub-bucket is sized to have enough bits for the requested
  // 10^precision accuracy.
  int sub_bucket_count_magnitude =
      Bits::log2Ceiling(largest_value_with_single_unit_resolution);
  subBucketHalfCountMagnitude_ =
      (sub_bucket_count_magnitude >= 1) ? sub_bucket_count_magnitude - 1 : 0;

  // subBucketCount_ is approx. 10^num_sig_digits (as a power of 2)
  subBucketCount_ = pow(2.0, subBucketHalfCountMagnitude_ + 1);
  subBucketMask_ = subBucketCount_ - 1;
  subBucketHalfCount_ = subBucketCount_ / 2;

  // The buckets take care of the magnitude.
  // Determine exponent range needed to support the trackable value with no
  // overflow:
  uint64_t trackable_value = subBucketCount_ - 1;
  int buckets_needed = 1;
  while (trackable_value < highestTrackableValue_) {
    trackable_value <<= 1;
    buckets_needed++;
  }
  bucketCount_ = buckets_needed;

  countsArrayLength_ = (bucketCount_ + 1) * subBucketHalfCount_;
  counts_.reset(new Atomic64[countsArrayLength_]()); // value-initialized
}

void HdrHistogram::increment(int64_t value) {
  incrementBy(value, 1);
}

void HdrHistogram::incrementBy(int64_t value, int64_t count) {
  shared_lock<rw_spinlock> lock(histogramMutex_);
  DCHECK_GE(value, 0);
  DCHECK_GE(count, 0);

  // Dissect the value into bucket and sub-bucket parts, and derive index into
  // counts array:
  int bucket_index = bucketIndex(value);
  int sub_bucket_index = subBucketIndex(value, bucket_index);
  int counts_index = countsArrayIndex(bucket_index, sub_bucket_index);

  // Increment bucket, total, and sum.
  NoBarrier_AtomicIncrement(&counts_[counts_index], count);
  NoBarrier_AtomicIncrement(&totalCount_, count);
  NoBarrier_AtomicIncrement(&totalSum_, value * count);

  // Update min, if needed.
  {
    Atomic64 min_val;
    while (PREDICT_FALSE(value < (min_val = minValue()))) {
      Atomic64 old_val = NoBarrier_CompareAndSwap(&minValue_, min_val, value);
      if (PREDICT_TRUE(old_val == min_val)) {
        break; // CAS success.
      }
    }
  }

  // Update max, if needed.
  {
    Atomic64 max_val;
    while (PREDICT_FALSE(value > (max_val = maxValue()))) {
      Atomic64 old_val = NoBarrier_CompareAndSwap(&maxValue_, max_val, value);
      if (PREDICT_TRUE(old_val == max_val)) {
        break; // CAS success.
      }
    }
  }
}

void HdrHistogram::incrementWithExpectedInterval(
    int64_t value,
    int64_t expected_interval_between_samples) {
  increment(value);
  if (expected_interval_between_samples <= 0) {
    return;
  }
  for (int64_t missing_value = value - expected_interval_between_samples;
       missing_value >= expected_interval_between_samples;
       missing_value -= expected_interval_between_samples) {
    increment(missing_value);
  }
}

////////////////////////////////////

int HdrHistogram::bucketIndex(uint64_t value) const {
  if (PREDICT_FALSE(value > highestTrackableValue_)) {
    value = highestTrackableValue_;
  }
  // Here we are calculating the power-of-2 magnitude of the value with a
  // correction for precision in the first bucket.
  // Smallest power of 2 containing value.
  int pow2ceiling = Bits::log2Ceiling64(value | subBucketMask_);
  return pow2ceiling - (subBucketHalfCountMagnitude_ + 1);
}

int HdrHistogram::subBucketIndex(uint64_t value, int bucket_index) const {
  if (PREDICT_FALSE(value > highestTrackableValue_)) {
    value = highestTrackableValue_;
  }
  // We hack off the magnitude and are left with only the relevant precision
  // portion, which gives us a direct index into the sub-bucket. TODO: Right??
  return static_cast<int>(value >> bucket_index);
}

int HdrHistogram::countsArrayIndex(int bucket_index, int sub_bucket_index)
    const {
  DCHECK(sub_bucket_index < subBucketCount_);
  DCHECK(bucket_index < bucketCount_);
  DCHECK(bucket_index == 0 || (sub_bucket_index >= subBucketHalfCount_));
  // Calculate the index for the first entry in the bucket:
  // (The following is the equivalent of ((bucket_index + 1) *
  // subBucketHalfCount_) ):
  int bucket_base_index = (bucket_index + 1) << subBucketHalfCountMagnitude_;
  // Calculate the offset in the bucket:
  int offset_in_bucket = sub_bucket_index - subBucketHalfCount_;
  return bucket_base_index + offset_in_bucket;
}

uint64_t HdrHistogram::countAt(int bucket_index, int sub_bucket_index) const {
  return counts_[countsArrayIndex(bucket_index, sub_bucket_index)];
}

uint64_t HdrHistogram::countInBucketForValue(uint64_t value) const {
  int bucket_index = bucketIndex(value);
  int sub_bucket_index = subBucketIndex(value, bucket_index);
  return countAt(bucket_index, sub_bucket_index);
}

uint64_t HdrHistogram::valueFromIndex(int bucket_index, int sub_bucket_index) {
  return static_cast<uint64_t>(sub_bucket_index) << bucket_index;
}

////////////////////////////////////

uint64_t HdrHistogram::sizeOfEquivalentValueRange(uint64_t value) const {
  int bucket_index = bucketIndex(value);
  int sub_bucket_index = subBucketIndex(value, bucket_index);
  uint64_t distance_to_next_value =
      (1
       << ((sub_bucket_index >= subBucketCount_) ? (bucket_index + 1)
                                                 : bucket_index));
  return distance_to_next_value;
}

uint64_t HdrHistogram::lowestEquivalentValue(uint64_t value) const {
  int bucket_index = bucketIndex(value);
  int sub_bucket_index = subBucketIndex(value, bucket_index);
  uint64_t this_value_base_level =
      valueFromIndex(bucket_index, sub_bucket_index);
  return this_value_base_level;
}

uint64_t HdrHistogram::highestEquivalentValue(uint64_t value) const {
  return nextNonEquivalentValue(value) - 1;
}

uint64_t HdrHistogram::medianEquivalentValue(uint64_t value) const {
  return (
      lowestEquivalentValue(value) + (sizeOfEquivalentValueRange(value) >> 1));
}

uint64_t HdrHistogram::nextNonEquivalentValue(uint64_t value) const {
  return lowestEquivalentValue(value) + sizeOfEquivalentValueRange(value);
}

bool HdrHistogram::valuesAreEquivalent(uint64_t value1, uint64_t value2) const {
  return (lowestEquivalentValue(value1) == lowestEquivalentValue(value2));
}

uint64_t HdrHistogram::minValue() const {
  if (PREDICT_FALSE(totalCount() == 0)) {
    return 0;
  }
  return NoBarrier_Load(&minValue_);
}

uint64_t HdrHistogram::maxValue() const {
  if (PREDICT_FALSE(totalCount() == 0)) {
    return 0;
  }
  return NoBarrier_Load(&maxValue_);
}

double HdrHistogram::meanValue() const {
  uint64_t count = totalCount();
  if (PREDICT_FALSE(count == 0)) {
    return 0.0;
  }
  return static_cast<double>(totalSum()) / count;
}

uint64_t HdrHistogram::valueAtPercentile(double percentile) const {
  uint64_t count = totalCount();
  if (PREDICT_FALSE(count == 0)) {
    return 0;
  }

  static constexpr long double k100Percent = 100.0L;
  long double requested_percentile = std::min(
      static_cast<long double>(percentile),
      k100Percent); // Truncate down to 100%
  uint64_t count_at_percentile = std::llroundl(
      ((requested_percentile / k100Percent) * static_cast<long double>(count)));
  // Make sure we at least reach the first recorded entry
  count_at_percentile = std::max(count_at_percentile, static_cast<uint64_t>(1));

  uint64_t total_to_current_iJ = 0;
  for (int i = 0; i < bucketCount_; i++) {
    int j = (i == 0) ? 0 : (subBucketCount_ / 2);
    for (; j < subBucketCount_; j++) {
      total_to_current_iJ += countAt(i, j);
      if (total_to_current_iJ >= count_at_percentile) {
        uint64_t valueAtIndex = valueFromIndex(i, j);
        return valueAtIndex;
      }
    }
  }

  LOG(DFATAL)
      << "Fell through while iterating, likely concurrent modification of histogram";
  return 0;
}

void HdrHistogram::resetHistogram() {
  std::lock_guard<rw_spinlock> lock(histogramMutex_);
  totalCount_ = 0;
  totalSum_ = 0;
  minValue_ = std::numeric_limits<Atomic64>::max();
  maxValue_ = 0;
  counts_.reset(new Atomic64[countsArrayLength_]());
}

///////////////////////////////////////////////////////////////////////
// AbstractHistogramIterator
///////////////////////////////////////////////////////////////////////

AbstractHistogramIterator::AbstractHistogramIterator(
    const HdrHistogram* histogram)
    : histogram_(CHECK_NOTNULL(histogram)),
      cur_iter_val_(),
      histogram_total_count_(histogram_->totalCount()),
      current_bucket_index_(0),
      current_sub_bucket_index_(0),
      current_value_at_index_(0),
      next_bucket_index_(0),
      next_sub_bucket_index_(1),
      next_value_at_index_(1),
      prev_value_iterated_to_(0),
      total_count_to_prev_index_(0),
      total_count_to_current_index_(0),
      total_value_to_current_index_(0),
      count_at_this_value_(0),
      fresh_sub_bucket_(true) {}

bool AbstractHistogramIterator::hasNext() const {
  return total_count_to_current_index_ < histogram_total_count_;
}

Status AbstractHistogramIterator::next(HistogramIterationValue* value) {
  if (histogram_->totalCount() != histogram_total_count_) {
    return Status::IllegalState(
        "Concurrently modified histogram while traversing it");
  }

  // Move through the sub buckets and buckets until we hit the next reporting
  // level:
  while (!exhaustedSubBuckets()) {
    count_at_this_value_ =
        histogram_->countAt(current_bucket_index_, current_sub_bucket_index_);
    if (fresh_sub_bucket_) { // Don't add unless we've incremented since last
                             // bucket...
      total_count_to_current_index_ += count_at_this_value_;
      total_value_to_current_index_ += count_at_this_value_ *
          histogram_->medianEquivalentValue(current_value_at_index_);
      fresh_sub_bucket_ = false;
    }
    if (reachedIterationLevel()) {
      uint64_t curValueIteratedTo = valueIteratedTo();

      // Update iterator value.
      cur_iter_val_.valueIteratedTo = curValueIteratedTo;
      cur_iter_val_.valueIteratedFrom = prev_value_iterated_to_;
      cur_iter_val_.countAtValueIteratedTo = count_at_this_value_;
      cur_iter_val_.countAddedInThisIterationStep =
          (total_count_to_current_index_ - total_count_to_prev_index_);
      cur_iter_val_.totalCountToThisValue = total_count_to_current_index_;
      cur_iter_val_.totalValueToThisValue = total_value_to_current_index_;
      cur_iter_val_.percentile =
          ((100.0 * total_count_to_current_index_) / histogram_total_count_);
      cur_iter_val_.percentileLevelIteratedTo = percentileIteratedTo();

      prev_value_iterated_to_ = curValueIteratedTo;
      total_count_to_prev_index_ = total_count_to_current_index_;
      // Move the next percentile reporting level forward.
      incrementIterationLevel();

      *value = cur_iter_val_;
      return Status::OK();
    }
    incrementSubBucket();
  }
  return Status::IllegalState(
      "Histogram array index out of bounds while traversing");
}

double AbstractHistogramIterator::percentileIteratedTo() const {
  return (100.0 * static_cast<double>(total_count_to_current_index_)) /
      histogram_total_count_;
}

double AbstractHistogramIterator::percentileIteratedFrom() const {
  return (100.0 * static_cast<double>(total_count_to_prev_index_)) /
      histogram_total_count_;
}

uint64_t AbstractHistogramIterator::valueIteratedTo() const {
  return histogram_->highestEquivalentValue(current_value_at_index_);
}

bool AbstractHistogramIterator::exhaustedSubBuckets() const {
  return (current_bucket_index_ >= histogram_->bucketCount_);
}

void AbstractHistogramIterator::incrementSubBucket() {
  fresh_sub_bucket_ = true;
  // Take on the next index:
  current_bucket_index_ = next_bucket_index_;
  current_sub_bucket_index_ = next_sub_bucket_index_;
  current_value_at_index_ = next_value_at_index_;
  // Figure out the next next index:
  next_sub_bucket_index_++;
  if (next_sub_bucket_index_ >= histogram_->subBucketCount_) {
    next_sub_bucket_index_ = histogram_->subBucketHalfCount_;
    next_bucket_index_++;
  }
  next_value_at_index_ =
      HdrHistogram::valueFromIndex(next_bucket_index_, next_sub_bucket_index_);
}

///////////////////////////////////////////////////////////////////////
// RecordedValuesIterator
///////////////////////////////////////////////////////////////////////

RecordedValuesIterator::RecordedValuesIterator(const HdrHistogram* histogram)
    : AbstractHistogramIterator(histogram),
      visited_sub_bucket_index_(-1),
      visited_bucket_index_(-1) {}

void RecordedValuesIterator::incrementIterationLevel() {
  visited_sub_bucket_index_ = current_sub_bucket_index_;
  visited_bucket_index_ = current_bucket_index_;
}

bool RecordedValuesIterator::reachedIterationLevel() const {
  uint64_t current_ij_count =
      histogram_->countAt(current_bucket_index_, current_sub_bucket_index_);
  return current_ij_count != 0 &&
      ((visited_sub_bucket_index_ != current_sub_bucket_index_) ||
       (visited_bucket_index_ != current_bucket_index_));
}

///////////////////////////////////////////////////////////////////////
// PercentileIterator
///////////////////////////////////////////////////////////////////////

PercentileIterator::PercentileIterator(
    const HdrHistogram* histogram,
    int percentile_ticks_per_half_distance)
    : AbstractHistogramIterator(histogram),
      percentile_ticks_per_half_distance_(percentile_ticks_per_half_distance),
      percentile_level_to_iterate_to_(0.0),
      percentile_level_to_iterate_from_(0.0),
      reached_last_recorded_value_(false) {}

bool PercentileIterator::hasNext() const {
  if (AbstractHistogramIterator::hasNext()) {
    return true;
  }
  // We want one additional last step to 100%
  if (!reached_last_recorded_value_ && (histogram_total_count_ > 0)) {
    const_cast<PercentileIterator*>(this)->percentile_level_to_iterate_to_ =
        100.0;
    const_cast<PercentileIterator*>(this)->reached_last_recorded_value_ = true;
    return true;
  }
  return false;
}

double PercentileIterator::percentileIteratedTo() const {
  return percentile_level_to_iterate_to_;
}

double PercentileIterator::percentileIteratedFrom() const {
  return percentile_level_to_iterate_from_;
}

void PercentileIterator::incrementIterationLevel() {
  percentile_level_to_iterate_from_ = percentile_level_to_iterate_to_;
  // TODO: Can this expression be simplified?
  uint64_t percentile_reporting_ticks = percentile_ticks_per_half_distance_ *
      static_cast<uint64_t>(pow(
          2.0,
          static_cast<int>(
              log(100.0 / (100.0 - (percentile_level_to_iterate_to_))) /
              log(2)) +
              1));
  percentile_level_to_iterate_to_ += 100.0 / percentile_reporting_ticks;
}

bool PercentileIterator::reachedIterationLevel() const {
  if (count_at_this_value_ == 0) {
    return false;
  }
  double current_percentile =
      (100.0 * static_cast<double>(total_count_to_current_index_)) /
      histogram_total_count_;
  return (current_percentile >= percentile_level_to_iterate_to_);
}

} // namespace kudu
