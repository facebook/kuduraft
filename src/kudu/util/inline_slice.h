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
#ifndef KUDU_UTIL_INLINE_SLICE_H
#define KUDU_UTIL_INLINE_SLICE_H

#include <bit>

#include "kudu/gutil/atomicops.h"
#include "kudu/util/memory/arena.h"

namespace kudu {

#if __BYTE_ORDER != __LITTLE_ENDIAN
#error This needs to be ported for big endian
#endif

// Class which represents short strings inline, and stores longer ones
// by instead storing a pointer.
//
// Internal format:
// The buffer must be at least as large as a pointer (eg 8 bytes for 64-bit).
// Let ptr = bit-casting the first 8 bytes as a pointer:
// If buf_[0] < 0xff:
//   buf_[0] == length of stored data
//   buf_[1..1 + buf_[0]] == inline data
// If buf_[0] == 0xff:
//   buf_[1..sizeof(uint8_t *)] == pointer to indirect data, minus the MSB.
//   buf_[sizeof(uint8_t *)..] = unused
//     TODO: we could store a prefix of the indirect data in this unused space
//     in the future, which might be able to short-circuit some comparisons
//
// The indirect data which is pointed to is stored as a 4 byte length followed
// by the actual data.
//
// This class relies on the fact that the most significant bit of any x86
// pointer is 0 (i.e pointers only use the bottom 48 bits)
//
// If ATOMIC is true, then this class has the semantics that readers will never
// see invalid pointers, even in the case of concurrent access. However, they
// _may_ see invalid *data*. That is to say, calling 'asSlice()' will always
// return a slice which points to a valid memory region -- the memory region may
// contain garbage but will not cause a segfault on access.
//
// These ATOMIC semantics may seem too loose to be useful, but can be used in
// optimistic concurrency control schemes -- so long as accessing the slice
// doesn't produce a segfault, it's OK to read bad data on a race because the
// higher-level concurrency control will cause a retry.
template <size_t StorageSize, bool Atomic = false>
class InlineSlice {
 private:
  enum {
    kPointerByteWidth = sizeof(uintptr_t),
    kPointerBitWidth = kPointerByteWidth * 8,
    kMaxInlineData = StorageSize - 1
  };

  static_assert(
      StorageSize >= kPointerByteWidth,
      "InlineSlice storage size must be greater than the width of a pointer");
  static_assert(
      StorageSize <= 256,
      "InlineSlice storage size must be less than 256 bytes");

 public:
  InlineSlice() {}

  inline const Slice asSlice() const ATTRIBUTE_ALWAYS_INLINE {
    DiscriminatedPointer dptr = loadValue();

    if (dptr.isIndirect()) {
      const uint8_t* indirData = reinterpret_cast<const uint8_t*>(
          dptr.pointer); // NOLINT(performance-no-int-to-ptr): Converting stored
                         // pointer bits back to pointer for indirect data
                         // access
      uint32_t len = *reinterpret_cast<const uint32_t*>(indirData);
      indirData += sizeof(uint32_t);
      return Slice(indirData, static_cast<size_t>(len));
    }
    uint8_t len = dptr.discriminator;
    DCHECK_LE(len, StorageSize - 1);
    return Slice(&buf_[1], len);
  }

  template <class ArenaType>
  void set(const Slice& src, ArenaType* allocArena) {
    set(src.data(), src.size(), allocArena);
  }

  template <class ArenaType>
  void set(const uint8_t* src, size_t len, ArenaType* allocArena) {
    if (len <= kMaxInlineData) {
      if (Atomic) {
        // If atomic, we need to make sure that we store the discriminator
        // before we copy in any data. Otherwise the data would overwrite
        // part of a pointer and a reader might see an invalid address.
        DiscriminatedPointer dptr;
        dptr.discriminator = len;
        dptr.pointer = 0; // will be overwritten
        // "Acquire" ensures that the later memcpy doesn't reorder above the
        // set of the discriminator bit.
        base::subtle::Acquire_Store(
            reinterpret_cast<volatile AtomicWord*>(buf_),
            std::bit_cast<uintptr_t>(dptr));
      } else {
        buf_[0] = len;
      }
      memcpy(&buf_[1], src, len);

    } else {
      // TODO: if already indirect and the current storage has enough space,
      // just reuse that.

      // Set up the pointed-to data before setting a pointer to it. This ensures
      // that readers never see a pointer to an invalid region (i.e one without
      // a proper length header).
      void* inArena =
          CHECK_NOTNULL(allocArena->allocateBytes(len + sizeof(uint32_t)));
      *reinterpret_cast<uint32_t*>(inArena) = len;
      memcpy(reinterpret_cast<uint8_t*>(inArena) + sizeof(uint32_t), src, len);
      setPtr(inArena);
    }
  }

 private:
  struct DiscriminatedPointer {
    uint8_t discriminator : 8;
    uintptr_t pointer : 54;

    bool isIndirect() const {
      return discriminator == 0xff;
    }
  };

  DiscriminatedPointer loadValue() const {
    if (Atomic) {
      // Load with "Acquire" semantics -- if we load a pointer, this ensures
      // that we also see the pointed-to data.
      uintptr_t ptrVal = base::subtle::Acquire_Load(
          reinterpret_cast<volatile const AtomicWord*>(buf_));
      return std::bit_cast<DiscriminatedPointer>(ptrVal);
    } else {
      DiscriminatedPointer ret;
      memcpy(&ret, buf_, sizeof(ret));
      return ret;
    }
  }

  // Set the internal storage to be an indirect pointer to the given
  // address.
  void setPtr(void* ptr) {
    uintptr_t ptrInt = reinterpret_cast<uintptr_t>(ptr);
    DCHECK_EQ(ptrInt >> (kPointerBitWidth - 8), 0)
        << "bad pointer (should have 0x00 MSB): " << ptr;

    DiscriminatedPointer dptr;
    dptr.discriminator = 0xff;
    dptr.pointer = ptrInt;

    if (Atomic) {
      // Store with "Release" semantics -- this ensures that the pointed-to data
      // is visible to any readers who see this pointer.
      uintptr_t toStore = std::bit_cast<uintptr_t>(dptr);
      base::subtle::Release_Store(
          reinterpret_cast<volatile AtomicWord*>(buf_), toStore);
    } else {
      memcpy(&buf_[0], &dptr, sizeof(dptr));
    }
  }

  uint8_t buf_[StorageSize];

} PACKED;

} // namespace kudu

#endif
