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
#ifndef IMPALA_UTIL_BIT_STREAM_UTILS_H
#define IMPALA_UTIL_BIT_STREAM_UTILS_H

#include "kudu/gutil/port.h"
#include "kudu/util/bit-util.h"
#include "kudu/util/faststring.h"

namespace kudu {

// Utility class to write bit/byte streams.  This class can write data to either
// be bit packed or byte aligned (and a single stream that has a mix of both).
class BitWriter {
 public:
  // buffer: buffer to write bits to.
  explicit BitWriter(faststring* buffer) : buffer_(buffer) {
    clear();
  }

  void clear() {
    bufferedValues_ = 0;
    byteOffset_ = 0;
    bitOffset_ = 0;
    buffer_->clear();
  }

  // Returns a pointer to the underlying buffer
  faststring* buffer() const {
    return buffer_;
  }

  // The number of current bytes written, including the current byte (i.e. may
  // include a fraction of a byte). Includes buffered values.
  int bytesWritten() const {
    return byteOffset_ + BitUtil::ceil(bitOffset_, 8);
  }

  // Writes a value to bufferedValues_, flushing to buffer_ if necessary.  This
  // is bit packed. numBits must be <= 32. If 'v' is larger than 'numBits'
  // bits, the higher bits are ignored.
  void putValue(uint64_t v, int numBits);

  // Writes v to the next aligned byte using numBits. If T is larger than
  // numBits, the extra high-order bits will be ignored.
  template <typename T>
  void putAligned(T v, int numBits);

  // Write a Vlq encoded int to the buffer. The value is written byte aligned.
  // For more details on vlq: en.wikipedia.org/wiki/Variable-length_quantity
  void putVlqInt(int32_t v);

  // Get the index to the next aligned byte and advance the underlying buffer by
  // numBytes.
  size_t getByteIndexAndAdvance(int numBytes) {
    uint8_t* ptr = getNextBytePtr(numBytes);
    return ptr - buffer_->data();
  }

  // Get a pointer to the next aligned byte and advance the underlying buffer by
  // numBytes.
  uint8_t* getNextBytePtr(int numBytes);

  // Flushes all buffered values to the buffer. Call this when done writing to
  // the buffer. If 'align' is true, bufferedValues_ is reset and any future
  // writes will be written to the next byte boundary.
  void flush(bool align = false);

 private:
  // Bit-packed values are initially written to this variable before being
  // memcpy'd to buffer_. This is faster than writing values byte by byte
  // directly to buffer_.
  uint64_t bufferedValues_;

  faststring* buffer_;
  int byteOffset_; // Offset in buffer_
  int bitOffset_; // Offset in bufferedValues_
};

// Utility class to read bit/byte stream.  This class can read bits or bytes
// that are either byte aligned or not.  It also has utilities to read multiple
// bytes in one read (e.g. encoded int).
class BitReader {
 public:
  // 'buffer' is the buffer to read from.  The buffer's length is 'bufferLen'.
  BitReader(const uint8_t* buffer, int bufferLen);

  BitReader() : buffer_(NULL), maxBytes_(0) {}

  // Gets the next value from the buffer.  Returns true if 'v' could be read or
  // false if there are not enough bytes left. numBits must be <= 32.
  template <typename T>
  bool getValue(int numBits, T* v);

  // Reads a 'numBytes'-sized value from the buffer and stores it in 'v'. T
  // needs to be a little-endian native type and big enough to store
  // 'numBytes'. The value is assumed to be byte-aligned so the stream will be
  // advanced to the start of the next byte before 'v' is read. Returns false if
  // there are not enough bytes left.
  template <typename T>
  bool getAligned(int numBytes, T* v);

  // Reads a vlq encoded int from the stream.  The encoded int must start at the
  // beginning of a byte. Return false if there were not enough bytes in the
  // buffer.
  bool getVlqInt(int32_t* v);

  // Returns the number of bytes left in the stream, not including the current
  // byte (i.e., there may be an additional fraction of a byte).
  int bytesLeft() {
    return maxBytes_ - (byteOffset_ + BitUtil::ceil(bitOffset_, 8));
  }

  // Current position in the stream, by bit.
  int position() const {
    return byteOffset_ * 8 + bitOffset_;
  }

  // Rewind the stream by 'numBits' bits
  void rewind(int numBits);

  // Seek to a specific bit in the buffer
  void seekToBit(uint streamPosition);

  // Maximum byte length of a vlq encoded int
  static const int kMaxVlqByteLen = 5;

  bool isInitialized() const {
    return buffer_ != NULL;
  }

 private:
  // Used by seekToBit() and getValue() to fetch the
  // the next word into buffer_.
  void bufferValues();

  const uint8_t* buffer_;
  int maxBytes_;

  // Bytes are memcpy'd from buffer_ and values are read from this variable.
  // This is faster than reading values byte by byte directly from buffer_.
  uint64_t bufferedValues_;

  int byteOffset_; // Offset in buffer_
  int bitOffset_; // Offset in bufferedValues_
};

} // namespace kudu

#endif
