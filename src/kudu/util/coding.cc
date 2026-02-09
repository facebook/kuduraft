// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "kudu/util/coding.h"
#include "kudu/util/coding-inl.h"
#include "kudu/util/faststring.h"

namespace kudu {

void putVarint32(faststring* dst, uint32_t v) {
  uint8_t buf[5];
  uint8_t* ptr = inlineEncodeVarint32(buf, v);
  dst->append(buf, ptr - buf);
}

uint8_t* encodeVarint64(uint8_t* dst, uint64_t v) {
  static const int kB = 128;
  while (v >= kB) {
    *(dst++) = (v & (kB - 1)) | kB;
    v >>= 7;
  }
  *(dst++) = static_cast<uint8_t>(v);
  return dst;
}

void putFixed32(faststring* dst, uint32_t value) {
  inlinePutFixed32(dst, value);
}

void putFixed64(faststring* dst, uint64_t value) {
  inlinePutFixed64(dst, value);
}

void putVarint64(faststring* dst, uint64_t v) {
  uint8_t buf[10];
  uint8_t* ptr = encodeVarint64(buf, v);
  dst->append(buf, ptr - buf);
}

void putLengthPrefixedSlice(faststring* dst, const Slice& value) {
  putVarint32(dst, value.size());
  dst->append(value.data(), value.size());
}

void putFixed32LengthPrefixedSlice(faststring* dst, const Slice& value) {
  putFixed32(dst, value.size());
  dst->append(value.data(), value.size());
}

int varintLength(uint64_t v) {
  int len = 1;
  while (v >= 128) {
    v >>= 7;
    len++;
  }
  return len;
}

const uint8_t* getVarint32PtrFallback(
    const uint8_t* p,
    const uint8_t* limit,
    uint32_t* value) {
  uint32_t result = 0;
  for (uint32_t shift = 0; shift <= 28 && p < limit; shift += 7) {
    uint32_t byte = *p;
    p++;
    if (byte & 128) {
      // More bytes are present
      result |= ((byte & 127) << shift);
    } else {
      result |= (byte << shift);
      *value = result;
      return p;
    }
  }
  return nullptr;
}

bool getVarint32(Slice* input, uint32_t* value) {
  const uint8_t* p = input->data();
  const uint8_t* limit = p + input->size();
  const uint8_t* q = getVarint32Ptr(p, limit, value);
  if (q == nullptr) {
    return false;
  } else {
    *input = Slice(q, limit - q);
    return true;
  }
}

const uint8_t*
getVarint64Ptr(const uint8_t* p, const uint8_t* limit, uint64_t* value) {
  uint64_t result = 0;
  for (uint32_t shift = 0; shift <= 63 && p < limit; shift += 7) {
    uint64_t byte = *p;
    p++;
    if (byte & 128) {
      // More bytes are present
      result |= ((byte & 127) << shift);
    } else {
      result |= (byte << shift);
      *value = result;
      return p;
    }
  }
  return nullptr;
}

bool getVarint64(Slice* input, uint64_t* value) {
  const uint8_t* p = input->data();
  const uint8_t* limit = p + input->size();
  const uint8_t* q = getVarint64Ptr(p, limit, value);
  if (q == nullptr) {
    return false;
  } else {
    *input = Slice(q, limit - q);
    return true;
  }
}

const uint8_t*
getLengthPrefixedSlice(const uint8_t* p, const uint8_t* limit, Slice* result) {
  uint32_t len = 0;
  p = getVarint32Ptr(p, limit, &len);
  if (p == nullptr) {
    return nullptr;
  }
  if (p + len > limit) {
    return nullptr;
  }
  *result = Slice(p, len);
  return p + len;
}

bool getLengthPrefixedSlice(Slice* input, Slice* result) {
  uint32_t len = 0;
  if (getVarint32(input, &len) && input->size() >= len) {
    *result = Slice(input->data(), len);
    input->removePrefix(len);
    return true;
  } else {
    return false;
  }
}

} // namespace kudu
