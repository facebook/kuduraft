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
#ifndef IMPALA_UTIL_BIT_STREAM_UTILS_INLINE_H
#define IMPALA_UTIL_BIT_STREAM_UTILS_INLINE_H

#include <algorithm>

#include "glog/logging.h"
#include "kudu/util/alignment.h"
#include "kudu/util/bit-stream-utils.h"

namespace kudu {

inline void BitWriter::putValue(uint64_t v, int numBits) {
  DCHECK_LE(numBits, 64);
  // Truncate the higher-order bits. This is necessary to
  // support signed values.
  v &= ~0ULL >> (64 - numBits);

  bufferedValues_ |= v << bitOffset_;
  bitOffset_ += numBits;

  if (PREDICT_FALSE(bitOffset_ >= 64)) {
    // Flush bufferedValues_ and write out bits of v that did not fit
    buffer_->reserve(KUDU_ALIGN_UP(byteOffset_ + 8, 8));
    buffer_->resize(byteOffset_ + 8);
    DCHECK_LE(byteOffset_ + 8, buffer_->capacity());
    memcpy(buffer_->data() + byteOffset_, &bufferedValues_, 8);
    bufferedValues_ = 0;
    byteOffset_ += 8;
    bitOffset_ -= 64;
    bufferedValues_ =
        BitUtil::shiftRightZeroOnOverflow(v, (numBits - bitOffset_));
  }
  DCHECK_LT(bitOffset_, 64);
}

inline void BitWriter::flush(bool align) {
  int numBytes = BitUtil::ceil(bitOffset_, 8);
  buffer_->reserve(KUDU_ALIGN_UP(byteOffset_ + numBytes, 8));
  buffer_->resize(byteOffset_ + numBytes);
  DCHECK_LE(byteOffset_ + numBytes, buffer_->capacity());
  memcpy(buffer_->data() + byteOffset_, &bufferedValues_, numBytes);

  if (align) {
    bufferedValues_ = 0;
    byteOffset_ += numBytes;
    bitOffset_ = 0;
  }
}

inline uint8_t* BitWriter::getNextBytePtr(int numBytes) {
  flush(/* align */ true);
  buffer_->reserve(KUDU_ALIGN_UP(byteOffset_ + numBytes, 8));
  buffer_->resize(byteOffset_ + numBytes);
  uint8_t* ptr = buffer_->data() + byteOffset_;
  byteOffset_ += numBytes;
  DCHECK_LE(byteOffset_, buffer_->capacity());
  return ptr;
}

template <typename T>
inline void BitWriter::putAligned(T val, int numBytes) {
  DCHECK_LE(numBytes, sizeof(T));
  uint8_t* ptr = getNextBytePtr(numBytes);
  memcpy(ptr, &val, numBytes);
}

inline void BitWriter::putVlqInt(int32_t v) {
  while ((v & 0xFFFFFF80) != 0L) {
    putAligned<uint8_t>((v & 0x7F) | 0x80, 1);
    v >>= 7;
  }
  putAligned<uint8_t>(v & 0x7F, 1);
}

inline BitReader::BitReader(const uint8_t* buffer, int bufferLen)
    : buffer_(buffer),
      maxBytes_(bufferLen),
      bufferedValues_(0),
      byteOffset_(0),
      bitOffset_(0) {
  int numBytes = std::min(8, maxBytes_);
  memcpy(&bufferedValues_, buffer_ + byteOffset_, numBytes);
}

inline void BitReader::bufferValues() {
  int bytesRemaining = maxBytes_ - byteOffset_;
  if (PREDICT_TRUE(bytesRemaining >= 8)) {
    memcpy(&bufferedValues_, buffer_ + byteOffset_, 8);
  } else {
    memcpy(&bufferedValues_, buffer_ + byteOffset_, bytesRemaining);
  }
}

template <typename T>
inline bool BitReader::getValue(int numBits, T* v) {
  DCHECK_LE(numBits, 64);
  DCHECK_LE(numBits, sizeof(T) * 8);

  if (PREDICT_FALSE(byteOffset_ * 8 + bitOffset_ + numBits > maxBytes_ * 8)) {
    return false;
  }

  *v = BitUtil::trailingBits(bufferedValues_, bitOffset_ + numBits) >>
      bitOffset_;

  bitOffset_ += numBits;
  if (bitOffset_ >= 64) {
    byteOffset_ += 8;
    bitOffset_ -= 64;
    bufferValues();
    // Read bits of v that crossed into new bufferedValues_
    *v |= BitUtil::shiftLeftZeroOnOverflow(
        BitUtil::trailingBits(bufferedValues_, bitOffset_),
        (numBits - bitOffset_));
  }
  DCHECK_LE(bitOffset_, 64);
  return true;
}

inline void BitReader::rewind(int numBits) {
  bitOffset_ -= numBits;
  if (bitOffset_ >= 0) {
    return;
  }
  while (bitOffset_ < 0) {
    int seekBack = std::min(byteOffset_, 8);
    byteOffset_ -= seekBack;
    bitOffset_ += seekBack * 8;
  }
  // This should only be executed *if* rewinding by 'numBits'
  // make the existing bufferedValues_ invalid
  DCHECK_GE(byteOffset_, 0); // Check for underflow
  memcpy(&bufferedValues_, buffer_ + byteOffset_, 8);
}

inline void BitReader::seekToBit(uint streamPosition) {
  DCHECK_LE(streamPosition, maxBytes_ * 8);

  int delta = static_cast<int>(streamPosition) - position();
  if (delta == 0) {
    return;
  } else if (delta < 0) {
    rewind(position() - streamPosition);
  } else {
    bitOffset_ += delta;
    while (bitOffset_ >= 64) {
      byteOffset_ += 8;
      bitOffset_ -= 64;
      if (bitOffset_ < 64) {
        // This should only be executed if seeking to
        // 'streamPosition' makes the existing bufferedValues_
        // invalid.
        bufferValues();
      }
    }
  }
}

template <typename T>
inline bool BitReader::getAligned(int numBytes, T* v) {
  DCHECK_LE(numBytes, sizeof(T));
  int bytesRead = BitUtil::ceil(bitOffset_, 8);
  if (PREDICT_FALSE(byteOffset_ + bytesRead + numBytes > maxBytes_)) {
    return false;
  }

  // Advance byteOffset_ to next unread byte and read numBytes
  byteOffset_ += bytesRead;
  memcpy(v, buffer_ + byteOffset_, numBytes);
  byteOffset_ += numBytes;

  // Reset bufferedValues_
  bitOffset_ = 0;
  int bytesRemaining = maxBytes_ - byteOffset_;
  if (PREDICT_TRUE(bytesRemaining >= 8)) {
    memcpy(&bufferedValues_, buffer_ + byteOffset_, 8);
  } else {
    memcpy(&bufferedValues_, buffer_ + byteOffset_, bytesRemaining);
  }
  return true;
}

inline bool BitReader::getVlqInt(int32_t* v) {
  *v = 0;
  int shift = 0;
  int numBytes = 0;
  uint8_t byte = 0;
  do {
    if (!getAligned<uint8_t>(1, &byte)) {
      return false;
    }
    *v |= (byte & 0x7F) << shift;
    shift += 7;
    DCHECK_LE(++numBytes, kMaxVlqByteLen);
  } while ((byte & 0x80) != 0);
  return true;
}

} // namespace kudu

#endif
