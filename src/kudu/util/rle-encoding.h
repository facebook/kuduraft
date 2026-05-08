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
#ifndef IMPALA_RLE_ENCODING_H
#define IMPALA_RLE_ENCODING_H

#include <glog/logging.h>

#include "kudu/gutil/port.h"
#include "kudu/util/bit-stream-utils.inline.h"
#include "kudu/util/bit-util.h"

namespace kudu {

// Utility classes to do run length encoding (RLE) for fixed bit width values.
// If runs are sufficiently long, RLE is used, otherwise, the values are just
// bit-packed (literal encoding). For both types of runs, there is a
// byte-aligned indicator which encodes the length of the run and the type of
// the run. This encoding has the benefit that when there aren't any long enough
// runs, values are always decoded at fixed (can be precomputed) bit offsets OR
// both the value and the run length are byte aligned. This allows for very
// efficient decoding implementations. The encoding is:
//    encoded-block := run*
//    run := literal-run | repeated-run
//    literal-run := literal-indicator < literal bytes >
//    repeated-run := repeated-indicator < repeated value. padded to byte
//    boundary > literal-indicator := varint_encode( number_of_groups << 1 | 1)
//    repeated-indicator := varint_encode( number_of_repetitions << 1 )
//
// Each run is preceded by a varint. The varint's least significant bit is
// used to indicate whether the run is a literal run or a repeated run. The rest
// of the varint is used to determine the length of the run (eg how many times
// the value repeats).
//
// In the case of literal runs, the run length is always a multiple of 8 (i.e.
// encode in groups of 8), so that no matter the bit-width of the value, the
// sequence will end on a byte boundary without padding. Given that we know it
// is a multiple of 8, we store the number of 8-groups rather than the actual
// number of encoded ints. (This means that the total number of encoded values
// can not be determined from the encoded data, since the number of values in
// the last group may not be a multiple of 8). There is a break-even point when
// it is more storage efficient to do run length encoding.  For 1 bit-width
// values, that point is 8 values.  They require 2 bytes for both the repeated
// encoding or the literal encoding.  This value can always be computed based on
// the bit-width.
// TODO: think about how to use this for strings.  The bit packing isn't quite
// the same.
//
// Examples with bit-width 1 (eg encoding booleans):
// ----------------------------------------
// 100 1s followed by 100 0s:
// <varint(100 << 1)> <1, padded to 1 byte> <varint(100 << 1)> <0, padded to 1
// byte>
//  - (total 4 bytes)
//
// alternating 1s and 0s (200 total):
// 200 ints = 25 groups of 8
// <varint((25 << 1) | 1)> <25 bytes of values, bitpacked>
// (total 26 bytes, 1 byte overhead)
//

// Decoder class for RLE encoded data.
//
// NOTE: the encoded format does not have any length prefix or any other way of
// indicating that the encoded sequence ends at a certain point, so the Decoder
// methods may return some extra bits at the end before the read methods start
// to return 0/false.
template <typename T>
class RleDecoder {
 public:
  // Create a decoder object. buffer/bufferLen is the decoded data.
  // bit_width is the width of each value (before encoding).
  RleDecoder(const uint8_t* buffer, int bufferLen, int bitWidth)
      : bitReader_(buffer, bufferLen),
        bitWidth_(bitWidth),
        currentValue_(0),
        repeatCount_(0),
        literalCount_(0),
        rewindState_(kCantRewind) {
    DCHECK_GE(bitWidth_, 1);
    DCHECK_LE(bitWidth_, 64);
  }

  RleDecoder() {}

  // Skip n values, and returns the number of non-zero entries skipped.
  size_t skip(size_t toSkip);

  // Gets the next value.  Returns false if there are no more.
  bool get(T* val);

  // Gets the next run of the same 'val'. Returns 0 if there is no
  // more data to be decoded. Will return a run of at most 'maxRun'
  // values. If there are more values than this, the next call to
  // getNextRun will return more from the same run.
  size_t getNextRun(T* val, size_t maxRun);

 private:
  bool readHeader();

  enum RewindState { kRewindLiteral, kRewindRun, kCantRewind };

  BitReader bitReader_;
  int bitWidth_;
  uint64_t currentValue_;
  uint32_t repeatCount_;
  uint32_t literalCount_;
  RewindState rewindState_;
};

// Class to incrementally build the rle data.
// The encoding has two modes: encoding repeated runs and literal runs.
// If the run is sufficiently short, it is more efficient to encode as a literal
// run. This class does so by buffering 8 values at a time.  If they are not all
// the same they are added to the literal run.  If they are the same, they are
// added to the repeated run.  When we switch modes, the previous run is flushed
// out.
template <typename T>
class RleEncoder {
 public:
  // buffer: buffer to write bits to.
  // bit_width: max number of bits for value.
  // TODO: consider adding a min_repeated_run_length so the caller can control
  // when values should be encoded as repeated runs.  Currently this is derived
  // based on the bit_width, which can determine a storage optimal choice.
  explicit RleEncoder(faststring* buffer, int bitWidth)
      : bitWidth_(bitWidth), bitWriter_(buffer) {
    DCHECK_GE(bitWidth_, 1);
    DCHECK_LE(bitWidth_, 64);
    clear();
  }

  // Encode value. This value must be representable with bitWidth_ bits.
  void put(T value, size_t runLength = 1);

  // Flushes any pending values to the underlying buffer.
  // Returns the total number of bytes written
  int flush();

  // Resets all the state in the encoder.
  void clear();

  int32_t len() const {
    return bitWriter_.bytesWritten();
  }

 private:
  // Flushes any buffered values.  If this is part of a repeated run, this is
  // largely a no-op. If it is part of a literal run, this will call
  // flushLiteralRun, which writes out the buffered literal values. If 'done' is
  // true, the current run would be written even if it would normally have been
  // buffered more.  This should only be called at the end, when the encoder has
  // received all values even if it would normally continue to be buffered.
  void flushBufferedValues(bool done);

  // Flushes literal values to the underlying buffer.  If updateIndicatorByte,
  // then the current literal run is complete and the indicator byte is updated.
  void flushLiteralRun(bool updateIndicatorByte);

  // Flushes a repeated run to the underlying buffer.
  void flushRepeatedRun();

  // Number of bits needed to encode the value.
  const int bitWidth_;

  // Underlying buffer.
  BitWriter bitWriter_;

  // We need to buffer at most 8 values for literals.  This happens when the
  // bit_width is 1 (so 8 values fit in one byte).
  // TODO: generalize this to other bit widths
  uint64_t bufferedValues_[8];

  // Number of values in bufferedValues_
  int numBufferedValues_;

  // The current (also last) value that was written and the count of how
  // many times in a row that value has been seen.  This is maintained even
  // if we are in a literal run.  If the repeatCount_ get high enough, we
  // switch to encoding repeated runs.
  uint64_t currentValue_;
  int repeatCount_;

  // Number of literals in the current run.  This does not include the literals
  // that might be in bufferedValues_.  Only after we've got a group big enough
  // can we decide if they should part of the literalCount_ or repeatCount_
  int literalCount_;

  // Index of a byte in the underlying buffer that stores the indicator byte.
  // This is reserved as soon as we need a literal run but the value is written
  // when the literal run is complete. We maintain an index rather than a
  // pointer into the underlying buffer because the pointer value may become
  // invalid if the underlying buffer is resized.
  int literalIndicatorByteIdx_;
};

template <typename T>
inline bool RleDecoder<T>::readHeader() {
  DCHECK(bitReader_.isInitialized());
  if (PREDICT_FALSE(literalCount_ == 0 && repeatCount_ == 0)) {
    // Read the next run's indicator int, it could be a literal or repeated run
    // The int is encoded as a vlq-encoded value.
    int32_t indicatorValue = 0;
    bool result = bitReader_.getVlqInt(&indicatorValue);
    if (PREDICT_FALSE(!result)) {
      return false;
    }

    // lsb indicates if it is a literal run or repeated run
    bool isLiteral = indicatorValue & 1;
    if (isLiteral) {
      literalCount_ = (indicatorValue >> 1) * 8;
      DCHECK_GT(literalCount_, 0);
    } else {
      repeatCount_ = indicatorValue >> 1;
      DCHECK_GT(repeatCount_, 0);
      bool result2 = bitReader_.getAligned<T>(
          BitUtil::ceil(bitWidth_, 8), reinterpret_cast<T*>(&currentValue_));
      DCHECK(result2);
    }
  }
  return true;
}

template <typename T>
inline bool RleDecoder<T>::get(T* val) {
  DCHECK(bitReader_.isInitialized());
  if (PREDICT_FALSE(!readHeader())) {
    return false;
  }

  if (PREDICT_TRUE(repeatCount_ > 0)) {
    *val = currentValue_;
    --repeatCount_;
    rewindState_ = kRewindRun;
  } else {
    DCHECK(literalCount_ > 0);
    bool result = bitReader_.getValue(bitWidth_, val);
    DCHECK(result);
    --literalCount_;
    rewindState_ = kRewindLiteral;
  }

  return true;
}

template <typename T>
inline size_t RleDecoder<T>::getNextRun(T* val, size_t maxRun) {
  DCHECK(bitReader_.isInitialized());
  DCHECK_GT(maxRun, 0);
  size_t ret = 0;
  size_t rem = maxRun;
  while (readHeader()) {
    if (PREDICT_TRUE(repeatCount_ > 0)) {
      if (PREDICT_FALSE(ret > 0 && *val != currentValue_)) {
        return ret;
      }
      *val = currentValue_;
      if (repeatCount_ >= rem) {
        // The next run is longer than the amount of remaining data
        // that the caller wants to read. Only consume it partially.
        repeatCount_ -= rem;
        ret += rem;
        return ret;
      }
      ret += repeatCount_;
      rem -= repeatCount_;
      repeatCount_ = 0;
    } else {
      DCHECK(literalCount_ > 0);
      if (ret == 0) {
        bool hasMore = bitReader_.getValue(bitWidth_, val);
        DCHECK(hasMore);
        literalCount_--;
        ret++;
        rem--;
      }

      while (literalCount_ > 0) {
        bool result = bitReader_.getValue(bitWidth_, &currentValue_);
        DCHECK(result);
        if (currentValue_ != *val || rem == 0) {
          bitReader_.rewind(bitWidth_);
          return ret;
        }
        ret++;
        rem--;
        literalCount_--;
      }
    }
  }
  return ret;
}

template <typename T>
inline size_t RleDecoder<T>::skip(size_t toSkip) {
  DCHECK(bitReader_.isInitialized());

  size_t setCount = 0;
  while (toSkip > 0) {
    bool result = readHeader();
    DCHECK(result);

    if (PREDICT_TRUE(repeatCount_ > 0)) {
      size_t nskip = (repeatCount_ < toSkip) ? repeatCount_ : toSkip;
      repeatCount_ -= nskip;
      toSkip -= nskip;
      if (currentValue_ != 0) {
        setCount += nskip;
      }
    } else {
      DCHECK(literalCount_ > 0);
      size_t nskip = (literalCount_ < toSkip) ? literalCount_ : toSkip;
      literalCount_ -= nskip;
      toSkip -= nskip;
      for (; nskip > 0; nskip--) {
        T value = 0;
        bool result2 = bitReader_.getValue(bitWidth_, &value);
        DCHECK(result2);
        if (value != 0) {
          setCount++;
        }
      }
    }
  }
  return setCount;
}

// This function buffers input values 8 at a time.  After seeing all 8 values,
// it decides whether they should be encoded as a literal or repeated run.
template <typename T>
inline void RleEncoder<T>::put(T value, size_t runLength) {
  DCHECK(bitWidth_ == 64 || value < (1LL << bitWidth_));

  // TODO(perf): remove the loop and use the repeatCount_
  for (; runLength > 0; runLength--) {
    if (PREDICT_TRUE(currentValue_ == value)) {
      ++repeatCount_;
      if (repeatCount_ > 8) {
        // This is just a continuation of the current run, no need to buffer the
        // values.
        // Note that this is the fast path for long repeated runs.
        continue;
      }
    } else {
      if (repeatCount_ >= 8) {
        // We had a run that was long enough but it has ended.  Flush the
        // current repeated run.
        DCHECK_EQ(literalCount_, 0);
        flushRepeatedRun();
      }
      repeatCount_ = 1;
      currentValue_ = value;
    }

    bufferedValues_[numBufferedValues_] = value;
    if (++numBufferedValues_ == 8) {
      DCHECK_EQ(literalCount_ % 8, 0);
      flushBufferedValues(false);
    }
  }
}

template <typename T>
inline void RleEncoder<T>::flushLiteralRun(bool updateIndicatorByte) {
  if (literalIndicatorByteIdx_ < 0) {
    // The literal indicator byte has not been reserved yet, get one now.
    literalIndicatorByteIdx_ = bitWriter_.getByteIndexAndAdvance(1);
    DCHECK_GE(literalIndicatorByteIdx_, 0);
  }

  // Write all the buffered values as bit packed literals
  for (int i = 0; i < numBufferedValues_; ++i) {
    bitWriter_.putValue(bufferedValues_[i], bitWidth_);
  }
  numBufferedValues_ = 0;

  if (updateIndicatorByte) {
    // At this point we need to write the indicator byte for the literal run.
    // We only reserve one byte, to allow for streaming writes of literal
    // values. The logic makes sure we flush literal runs often enough to not
    // overrun the 1 byte.
    int numGroups = BitUtil::ceil(literalCount_, 8);
    int32_t indicatorValue = (numGroups << 1) | 1;
    DCHECK_EQ(indicatorValue & 0xFFFFFF00, 0);
    bitWriter_.buffer()->data()[literalIndicatorByteIdx_] = indicatorValue;
    literalIndicatorByteIdx_ = -1;
    literalCount_ = 0;
  }
}

template <typename T>
inline void RleEncoder<T>::flushRepeatedRun() {
  DCHECK_GT(repeatCount_, 0);
  // The lsb of 0 indicates this is a repeated run
  int32_t indicatorValue = repeatCount_ << 1 | 0;
  bitWriter_.putVlqInt(indicatorValue);
  bitWriter_.putAligned(currentValue_, BitUtil::ceil(bitWidth_, 8));
  numBufferedValues_ = 0;
  repeatCount_ = 0;
}

// Flush the values that have been buffered.  At this point we decide whether
// we need to switch between the run types or continue the current one.
template <typename T>
inline void RleEncoder<T>::flushBufferedValues(bool done) {
  if (repeatCount_ >= 8) {
    // Clear the buffered values.  They are part of the repeated run now and we
    // don't want to flush them out as literals.
    numBufferedValues_ = 0;
    if (literalCount_ != 0) {
      // There was a current literal run.  All the values in it have been
      // flushed but we still need to update the indicator byte.
      DCHECK_EQ(literalCount_ % 8, 0);
      DCHECK_EQ(repeatCount_, 8);
      flushLiteralRun(true);
    }
    DCHECK_EQ(literalCount_, 0);
    return;
  }

  literalCount_ += numBufferedValues_;
  int numGroups = BitUtil::ceil(literalCount_, 8);
  if (numGroups + 1 >= (1 << 6)) {
    // We need to start a new literal run because the indicator byte we've
    // reserved cannot store more values.
    DCHECK_GE(literalIndicatorByteIdx_, 0);
    flushLiteralRun(true);
  } else {
    flushLiteralRun(done);
  }
  repeatCount_ = 0;
}

template <typename T>
inline int RleEncoder<T>::flush() {
  if (literalCount_ > 0 || repeatCount_ > 0 || numBufferedValues_ > 0) {
    bool allRepeat = literalCount_ == 0 &&
        (repeatCount_ == numBufferedValues_ || numBufferedValues_ == 0);
    // There is something pending, figure out if it's a repeated or literal run
    if (repeatCount_ > 0 && allRepeat) {
      flushRepeatedRun();
    } else {
      literalCount_ += numBufferedValues_;
      flushLiteralRun(true);
      repeatCount_ = 0;
    }
  }
  bitWriter_.flush();
  DCHECK_EQ(numBufferedValues_, 0);
  DCHECK_EQ(literalCount_, 0);
  DCHECK_EQ(repeatCount_, 0);
  return bitWriter_.bytesWritten();
}

template <typename T>
inline void RleEncoder<T>::clear() {
  currentValue_ = 0;
  repeatCount_ = 0;
  numBufferedValues_ = 0;
  literalCount_ = 0;
  literalIndicatorByteIdx_ = -1;
  bitWriter_.clear();
}

} // namespace kudu
#endif
