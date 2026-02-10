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

inline void BitWriter::putValue(uint64_t v, int num_bits) {
  DCHECK_LE(num_bits, 64);
  // Truncate the higher-order bits. This is necessary to
  // support signed values.
  v &= ~0ULL >> (64 - num_bits);

  bufferedValues_ |= v << bitOffset_;
  bitOffset_ += num_bits;

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
        BitUtil::shiftRightZeroOnOverflow(v, (num_bits - bitOffset_));
  }
  DCHECK_LT(bitOffset_, 64);
}

inline void BitWriter::flush(bool align) {
  int num_bytes = BitUtil::ceil(bitOffset_, 8);
  buffer_->reserve(KUDU_ALIGN_UP(byteOffset_ + num_bytes, 8));
  buffer_->resize(byteOffset_ + num_bytes);
  DCHECK_LE(byteOffset_ + num_bytes, buffer_->capacity());
  memcpy(buffer_->data() + byteOffset_, &bufferedValues_, num_bytes);

  if (align) {
    bufferedValues_ = 0;
    byteOffset_ += num_bytes;
    bitOffset_ = 0;
  }
}

inline uint8_t* BitWriter::getNextBytePtr(int num_bytes) {
  flush(/* align */ true);
  buffer_->reserve(KUDU_ALIGN_UP(byteOffset_ + num_bytes, 8));
  buffer_->resize(byteOffset_ + num_bytes);
  uint8_t* ptr = buffer_->data() + byteOffset_;
  byteOffset_ += num_bytes;
  DCHECK_LE(byteOffset_, buffer_->capacity());
  return ptr;
}

template <typename T>
inline void BitWriter::putAligned(T val, int num_bytes) {
  DCHECK_LE(num_bytes, sizeof(T));
  uint8_t* ptr = getNextBytePtr(num_bytes);
  memcpy(ptr, &val, num_bytes);
}

inline void BitWriter::putVlqInt(int32_t v) {
  while ((v & 0xFFFFFF80) != 0L) {
    putAligned<uint8_t>((v & 0x7F) | 0x80, 1);
    v >>= 7;
  }
  putAligned<uint8_t>(v & 0x7F, 1);
}

inline BitReader::BitReader(const uint8_t* buffer, int buffer_len)
    : buffer_(buffer),
      maxBytes_(buffer_len),
      bufferedValues_(0),
      byteOffset_(0),
      bitOffset_(0) {
  int num_bytes = std::min(8, maxBytes_);
  memcpy(&bufferedValues_, buffer_ + byteOffset_, num_bytes);
}

inline void BitReader::bufferValues() {
  int bytes_remaining = maxBytes_ - byteOffset_;
  if (PREDICT_TRUE(bytes_remaining >= 8)) {
    memcpy(&bufferedValues_, buffer_ + byteOffset_, 8);
  } else {
    memcpy(&bufferedValues_, buffer_ + byteOffset_, bytes_remaining);
  }
}

template <typename T>
inline bool BitReader::getValue(int num_bits, T* v) {
  DCHECK_LE(num_bits, 64);
  DCHECK_LE(num_bits, sizeof(T) * 8);

  if (PREDICT_FALSE(byteOffset_ * 8 + bitOffset_ + num_bits > maxBytes_ * 8)) {
    return false;
  }

  *v = BitUtil::trailingBits(bufferedValues_, bitOffset_ + num_bits) >>
      bitOffset_;

  bitOffset_ += num_bits;
  if (bitOffset_ >= 64) {
    byteOffset_ += 8;
    bitOffset_ -= 64;
    bufferValues();
    // Read bits of v that crossed into new bufferedValues_
    *v |= BitUtil::shiftLeftZeroOnOverflow(
        BitUtil::trailingBits(bufferedValues_, bitOffset_),
        (num_bits - bitOffset_));
  }
  DCHECK_LE(bitOffset_, 64);
  return true;
}

inline void BitReader::rewind(int num_bits) {
  bitOffset_ -= num_bits;
  if (bitOffset_ >= 0) {
    return;
  }
  while (bitOffset_ < 0) {
    int seek_back = std::min(byteOffset_, 8);
    byteOffset_ -= seek_back;
    bitOffset_ += seek_back * 8;
  }
  // This should only be executed *if* rewinding by 'num_bits'
  // make the existing bufferedValues_ invalid
  DCHECK_GE(byteOffset_, 0); // Check for underflow
  memcpy(&bufferedValues_, buffer_ + byteOffset_, 8);
}

inline void BitReader::seekToBit(uint stream_position) {
  DCHECK_LE(stream_position, maxBytes_ * 8);

  int delta = static_cast<int>(stream_position) - position();
  if (delta == 0) {
    return;
  } else if (delta < 0) {
    rewind(position() - stream_position);
  } else {
    bitOffset_ += delta;
    while (bitOffset_ >= 64) {
      byteOffset_ += 8;
      bitOffset_ -= 64;
      if (bitOffset_ < 64) {
        // This should only be executed if seeking to
        // 'stream_position' makes the existing bufferedValues_
        // invalid.
        bufferValues();
      }
    }
  }
}

template <typename T>
inline bool BitReader::getAligned(int num_bytes, T* v) {
  DCHECK_LE(num_bytes, sizeof(T));
  int bytes_read = BitUtil::ceil(bitOffset_, 8);
  if (PREDICT_FALSE(byteOffset_ + bytes_read + num_bytes > maxBytes_)) {
    return false;
  }

  // Advance byteOffset_ to next unread byte and read num_bytes
  byteOffset_ += bytes_read;
  memcpy(v, buffer_ + byteOffset_, num_bytes);
  byteOffset_ += num_bytes;

  // Reset bufferedValues_
  bitOffset_ = 0;
  int bytes_remaining = maxBytes_ - byteOffset_;
  if (PREDICT_TRUE(bytes_remaining >= 8)) {
    memcpy(&bufferedValues_, buffer_ + byteOffset_, 8);
  } else {
    memcpy(&bufferedValues_, buffer_ + byteOffset_, bytes_remaining);
  }
  return true;
}

inline bool BitReader::getVlqInt(int32_t* v) {
  *v = 0;
  int shift = 0;
  int num_bytes = 0;
  uint8_t byte = 0;
  do {
    if (!getAligned<uint8_t>(1, &byte)) {
      return false;
    }
    *v |= (byte & 0x7F) << shift;
    shift += 7;
    DCHECK_LE(++num_bytes, kMaxVlqByteLen);
  } while ((byte & 0x80) != 0);
  return true;
}

} // namespace kudu

#endif
