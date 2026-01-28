//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//

#pragma once

// NOTE: using stdint.h instead of cstdint because this file is supposed
//       to be processed by a compiler lacking C++11 support.
#include <stdint.h>

#include <cassert>
#include <cstddef>
#include <cstring>
#include <iosfwd>
#include <map>
#include <string>

#ifdef KUDU_HEADERS_USE_RICH_SLICE
#include "kudu/gutil/strings/fastmem.h"
#include "kudu/gutil/strings/stringpiece.h"
#include "kudu/util/faststring.h"
#endif
#ifdef KUDU_HEADERS_NO_STUBS
#include "kudu/gutil/port.h"
#endif
#include "kudu/util/kudu_export.h"

namespace kudu {

class Status;

/// @brief A wrapper around externally allocated data.
///
/// Slice is a simple structure containing a pointer into some external
/// storage and a size. The user of a Slice must ensure that the slice
/// is not used after the corresponding external storage has been
/// deallocated.
///
/// Multiple threads can invoke const methods on a Slice without
/// external synchronization, but if any of the threads may call a
/// non-const method, all threads accessing the same Slice must use
/// external synchronization.
///
/// Slices can be built around faststrings and StringPieces using constructors
/// with implicit casts. Both StringPieces and faststrings depend on a great
/// deal of gutil code.
class KUDU_EXPORT Slice {
 public:
  /// Create an empty slice.
  Slice() : data_(reinterpret_cast<const uint8_t*>("")), size_(0) {}

  /// Create a slice that refers to a @c uint8_t byte array.
  ///
  /// @param [in] d
  ///   The input array.
  /// @param [in] n
  ///   Number of bytes in the array.
  Slice(const uint8_t* d, size_t n) : data_(d), size_(n) {}

  /// Create a slice that refers to a @c char byte array.
  ///
  /// @param [in] d
  ///   The input array.
  /// @param [in] n
  ///   Number of bytes in the array.
  Slice(const char* d, size_t n)
      : data_(reinterpret_cast<const uint8_t*>(d)), size_(n) {}

  /// Create a slice that refers to the contents of the given string.
  ///
  /// @param [in] s
  ///   The input string.
  Slice(const std::string& s) // NOLINT(google-explicit-constructor)
      : data_(reinterpret_cast<const uint8_t*>(s.data())), size_(s.size()) {}

  /// Create a slice that refers to a C-string s[0,strlen(s)-1].
  ///
  /// @param [in] s
  ///   The input C-string.
  Slice(const char* s) // NOLINT(google-explicit-constructor)
      : data_(reinterpret_cast<const uint8_t*>(s)), size_(strlen(s)) {}

#ifdef KUDU_HEADERS_USE_RICH_SLICE
  /// Create a slice that refers to the contents of a faststring.
  ///
  /// @note Further appends to the faststring may invalidate this slice.
  ///
  /// @param [in] s
  ///   The input faststring.
  Slice(const faststring& s) // NOLINT(google-explicit-constructor)
      : data_(s.data()), size_(s.size()) {}

  /// Create a slice that refers to the contents of a string piece.
  ///
  /// @param [in] s
  ///   The input StringPiece.
  Slice(const StringPiece& s) // NOLINT(google-explicit-constructor)
      : data_(reinterpret_cast<const uint8_t*>(s.data())), size_(s.size()) {}
#endif

  /// @return A pointer to the beginning of the referenced data.
  const uint8_t* data() const {
    return data_;
  }

  /// @return A mutable pointer to the beginning of the referenced data.
  uint8_t* mutableData() {
    return const_cast<uint8_t*>(data_);
  }

  /// @return The length (in bytes) of the referenced data.
  size_t size() const {
    return size_;
  }

  /// @return @c true iff the length of the referenced data is zero.
  bool empty() const {
    return size_ == 0;
  }

  /// @pre n < size()
  ///
  /// @param [in] n
  ///   The index of the byte.
  /// @return the n-th byte in the referenced data.
  const uint8_t& operator[](size_t n) const {
    assert(n < size());
    return data_[n];
  }

  /// Change this slice to refer to an empty array.
  void clear() {
    data_ = reinterpret_cast<const uint8_t*>("");
    size_ = 0;
  }

  /// Drop the first "n" bytes from this slice.
  ///
  /// @pre n <= size()
  ///
  /// @note Only the base and bounds of the slice are changed;
  ///   the data is not modified.
  ///
  /// @param [in] n
  ///   Number of bytes that should be dropped from the beginning.
  void removePrefix(size_t n) {
    assert(n <= size());
    data_ += n;
    size_ -= n;
  }

  /// Truncate the slice to the given number of bytes.
  ///
  /// @pre n <= size()
  ///
  /// @note Only the base and bounds of the slice are changed;
  ///   the data is not modified.
  ///
  /// @param [in] n
  ///   The new size of the slice.
  void truncate(size_t n) {
    assert(n <= size());
    size_ = n;
  }

  /// Check that the slice has the expected size.
  ///
  /// @param [in] expectedSize
  /// @return Status::Corruption() iff size() != @c expectedSize
  Status checkSize(size_t expectedSize) const;

  /// @return A string that contains a copy of the referenced data.
  std::string ToString() const;

  /// Get printable representation of the data in the slice.
  ///
  /// @param [in] maxLen
  ///   The maximum number of bytes to output in the printable format;
  ///   @c 0 means no limit.
  /// @return A string with printable representation of the data.
  std::string ToDebugString(size_t maxLen = 0) const;

  /// Do a three-way comparison of the slice's data.
  ///
  /// @param [in] b
  ///   The other slice to compare with.
  /// @return Values are
  ///   @li <  0 iff "*this" <  "b"
  ///   @li == 0 iff "*this" == "b"
  ///   @li >  0 iff "*this" >  "b"
  int compare(const Slice& b) const;

  /// Check whether the slice starts with the given prefix.
  /// @param [in] x
  ///   The slice in question.
  /// @return @c true iff "x" is a prefix of "*this"
  bool startsWith(const Slice& x) const {
    return ((size_ >= x.size_) && (memEqual(data_, x.data_, x.size_)));
  }

  /// @brief Comparator struct, useful for ordered collections (like STL maps).
  struct Comparator {
    /// Compare two slices using Slice::compare()
    ///
    /// @param [in] a
    ///   The slice to call Slice::compare() at.
    /// @param [in] b
    ///   The slice to use as a parameter for Slice::compare().
    /// @return @c true iff @c a is less than @c b by Slice::compare().
    bool operator()(const Slice& a, const Slice& b) const {
      return a.compare(b) < 0;
    }
  };

 private:
  friend bool operator==(const Slice& x, const Slice& y);

  static bool memEqual(const void* a, const void* b, size_t n) {
#ifdef KUDU_HEADERS_USE_RICH_SLICE
    return strings::memeq(a, b, n);
#else
    return memcmp(a, b, n) == 0;
#endif
  }

  static int memCompare(const void* a, const void* b, size_t n) {
#ifdef KUDU_HEADERS_USE_RICH_SLICE
    return strings::fastmemcmpInlined(a, b, n);
#else
    return memcmp(a, b, n);
#endif
  }

  const uint8_t* data_;
  size_t size_;

  // Intentionally copyable
};

/// Check whether two slices are identical.
///
/// @param [in] x
///   One slice.
/// @param [in] y
///   Another slice.
/// @return @c true iff two slices contain byte-for-byte identical data.
inline bool operator==(const Slice& x, const Slice& y) {
  return (
      (x.size() == y.size()) &&
      (Slice::memEqual(x.data(), y.data(), x.size())));
}

/// Check whether two slices are not identical.
///
/// @param [in] x
///   One slice.
/// @param [in] y
///   Another slice.
/// @return @c true iff slices contain different data.
inline bool operator!=(const Slice& x, const Slice& y) {
  return !(x == y);
}

/// Output printable representation of the slice into the given output stream.
///
/// @param [out] o
///   The output stream to print the info.
/// @param [in] s
///   The slice to print.
/// @return Reference to the updated output stream.
inline std::ostream& operator<<(std::ostream& o, const Slice& s) {
  return o << s.ToDebugString(16); // should be enough for anyone...
}

inline int Slice::compare(const Slice& b) const {
  const int minLen = (size_ < b.size_) ? size_ : b.size_;
  int r = memCompare(data_, b.data_, minLen);
  if (r == 0) {
    if (size_ < b.size_) {
      r = -1;
    } else if (size_ > b.size_) {
      r = +1;
    }
  }
  return r;
}

// We don't run TSAN on this function because it makes it really slow and causes
// some test timeouts. This is only used on local buffers anyway, so we don't
// lose much by not checking it.
#ifdef KUDU_HEADERS_NO_STUBS
ATTRIBUTE_NO_SANITIZE_THREAD
#endif
bool IsAllZeros(const Slice& s);

/// @brief STL map whose keys are Slices.
///
/// An example of usage:
/// @code
///   typedef SliceMap<int>::type MySliceMap;
///
///   MySliceMap my_map;
///   my_map.insert(MySliceMap::value_type(a, 1));
///   my_map.insert(MySliceMap::value_type(b, 2));
///   my_map.insert(MySliceMap::value_type(c, 3));
///
///   for (const MySliceMap::value_type& pair : my_map) {
///     ...
///   }
/// @endcode
template <typename T>
struct SliceMap {
  /// A handy typedef for the slice map with appropriate comparison operator.
  using type = std::map<Slice, T, Slice::Comparator>;
};

} // namespace kudu
