// Copyright 2005 Google Inc.
//
// #status: RECOMMENDED
// #category: maps
// #summary: Utility functions for use with map-like containers.
//
// This file provides utility functions for use with STL map-like data
// structures, such as std::map and hash_map.
//
// The main functions in this file fall into the following categories:
//
// - Find*()
// - Contains*()
// - Insert*()
// - Lookup*()
//
// These functions often have "...OrDie" or "...OrDieNoPrint" variants. These
// variants will crash the process with a CHECK() failure on error, including
// the offending key/data in the log message. The NoPrint variants will not
// include the key/data in the log output under the assumption that it's not a
// printable type.
//
// Most functions are fairly self explanatory from their names, with the
// exception of Find*() vs Lookup*(). The Find functions typically use the map's
// .find() member function to locate and return the map's value type. The
// Lookup*() functions typically use the map's .insert() (yes, insert) member
// function to insert the given value if necessary and returns (usually a
// reference to) the map's value type for the found item.
//
// See the per-function comments for specifics.
//
// There are also a handful of functions for doing other miscellaneous things.
//
// A note on terminology:
//
// Map-like containers are collections of pairs. Like all STL containers they
// contain a few standard typedefs identifying the types of data they contain.
// Given the following map declaration:
//
//   map<string, int> my_map;
//
// the notable typedefs would be as follows:
//
//   - key_type    -- string
//   - value_type  -- pair<const string, int>
//   - mapped_type -- int
//
// Note that the map above contains two types of "values": the key-value pairs
// themselves (value_type) and the values within the key-value pairs
// (mapped_type). A value_type consists of a key_type and a mapped_type.
//
// The documentation below is written for programmers thinking in terms of keys
// and the (mapped_type) values associated with a given key.  For example, the
// statement
//
//   my_map["foo"] = 3;
//
// has a key of "foo" (type: string) with a value of 3 (type: int).
//
#pragma once

#include <stddef.h>

#include <tuple>
#include <utility>
#include <vector>

#include <glog/logging.h>

#include "kudu/gutil/logging-inl.h"

//
// Find*()
//

// Returns a const reference to the value associated with the given key if it
// exists. Crashes otherwise.
//
// This is intended as a replacement for operator[] as an rvalue (for reading)
// when the key is guaranteed to exist.
//
// Returns a const reference to the value associated with the given key if it
// exists, otherwise a const reference to the provided default value is
// returned.
//
// WARNING: If a temporary object is passed as the default "value," this
// function will return a reference to that temporary object, which will be
// destroyed by the end of the statement. Specifically, if you have a map with
// string values, and you pass a char* as the default "value," either use the
// returned value immediately or store it in a string (not string&). Details:
template <class Collection>
const typename Collection::mapped_type& FindWithDefault(
    const Collection& collection,
    const typename Collection::key_type& key,
    const typename Collection::mapped_type& value) {
  auto it = collection.find(key);
  if (it == collection.end()) {
    return value;
  }
  return it->second;
}

// Returns a pointer to the const value associated with the given key if it
// exists, or NULL otherwise.
template <class Collection>
const typename Collection::mapped_type* FindOrNull(
    const Collection& collection,
    const typename Collection::key_type& key) {
  auto it = collection.find(key);
  if (it == collection.end()) {
    return 0;
  }
  return &it->second;
}

// Same as above but returns a pointer to the non-const value.
template <class Collection>
typename Collection::mapped_type* FindOrNull(
    Collection& collection, // NOLINT
    const typename Collection::key_type& key) {
  auto it = collection.find(key);
  if (it == collection.end()) {
    return 0;
  }
  return &it->second;
}

// Returns a pointer to the const value associated with the greatest key
// that's less than or equal to the given key, or NULL if no such key exists.
template <class Collection>
const typename Collection::mapped_type* FindFloorOrNull(
    const Collection& collection,
    const typename Collection::key_type& key) {
  auto it = collection.upper_bound(key);
  if (it == collection.begin()) {
    return 0;
  }
  return &(--it)->second;
}

// Same as above but returns a pointer to the non-const value.
template <class Collection>
typename Collection::mapped_type* FindFloorOrNull(
    Collection& collection, // NOLINT
    const typename Collection::key_type& key) {
  auto it = collection.upper_bound(key);
  if (it == collection.begin()) {
    return 0;
  }
  return &(--it)->second;
}

// Returns a const-reference to the value associated with the greatest key
// that's less than or equal to the given key, or crashes if it does not exist.
template <class Collection>
const typename Collection::mapped_type& FindFloorOrDie(
    const Collection& collection,
    const typename Collection::key_type& key) {
  auto it = collection.upper_bound(key);
  CHECK(it != collection.begin());
  return (--it)->second;
}

// Same as above, but returns a non-const reference.
template <class Collection>
typename Collection::mapped_type& FindFloorOrDie(
    Collection& collection,
    const typename Collection::key_type& key) {
  auto it = collection.upper_bound(key);
  CHECK(it != collection.begin());
  return (--it)->second;
}

// Returns the pointer value associated with the given key. If none is found,
// NULL is returned. The function is designed to be used with a map of keys to
// pointers.
//
// This function does not distinguish between a missing key and a key mapped
// to a NULL value.
template <class Collection>
typename Collection::mapped_type FindPtrOrNull(
    const Collection& collection,
    const typename Collection::key_type& key) {
  auto it = collection.find(key);
  if (it == collection.end()) {
    return typename Collection::mapped_type(0);
  }
  return it->second;
}

// Same as above, except takes non-const reference to collection.
//
// This function is needed for containers that propagate constness to the
// pointee, such as boost::ptr_map.
template <class Collection>
typename Collection::mapped_type FindPtrOrNull(
    Collection& collection, // NOLINT
    const typename Collection::key_type& key) {
  auto it = collection.find(key);
  if (it == collection.end()) {
    return typename Collection::mapped_type(nullptr);
  }
  return it->second;
}

// FindPtrOrNull like function for maps whose value is a smart pointer like
// shared_ptr or unique_ptr. Returns the raw pointer contained in the smart
// pointer for the first found key, if it exists, or null if it doesn't.
template <class Collection>
typename Collection::mapped_type::element_type* FindPointeeOrNull(
    const Collection& collection, // NOLINT,
    const typename Collection::key_type& key) {
  auto it = collection.find(key);
  if (it == collection.end()) {
    return nullptr;
  }
  return it->second.get();
}

// Finds the value associated with the given key and copies it to *value (if not
// NULL). Returns false if the key was not found, true otherwise.
template <class Collection, class Key, class Value>
bool FindCopy(
    const Collection& collection,
    const Key& key,
    Value* const value) {
  auto it = collection.find(key);
  if (it == collection.end()) {
    return false;
  }
  if (value) {
    *value = it->second;
  }
  return true;
}

//
// Insert*()
//

// Inserts the given key-value pair into the collection. Returns true if the
// given key didn't previously exist. If the given key already existed in the
// map, its value is changed to the given "value" and false is returned.
template <class Collection>
bool InsertOrUpdate(
    Collection* const collection,
    const typename Collection::value_type& vt) {
  std::pair<typename Collection::iterator, bool> ret = collection->insert(vt);
  if (!ret.second) {
    // update
    ret.first->second = vt.second;
    return false;
  }
  return true;
}

// Same as above, except that the key and value are passed separately.
template <class Collection>
bool InsertOrUpdate(
    Collection* const collection,
    const typename Collection::key_type& key,
    const typename Collection::mapped_type& value) {
  return InsertOrUpdate(
      collection, typename Collection::value_type(key, value));
}

// Inserts the given key and value into the given collection iff the given key
// did NOT already exist in the collection. If the key previously existed in the
// collection, the value is not changed. Returns true if the key-value pair was
// inserted; returns false if the key was already present.
template <class Collection>
bool InsertIfNotPresent(
    Collection* const collection,
    const typename Collection::value_type& vt) {
  return collection->insert(vt).second;
}

// Same as above except the key and value are passed separately.
template <class Collection>
bool InsertIfNotPresent(
    Collection* const collection,
    const typename Collection::key_type& key,
    const typename Collection::mapped_type& value) {
  return InsertIfNotPresent(
      collection, typename Collection::value_type(key, value));
}

//
// Emplace*()
//
template <class Collection, class... Args>
bool EmplaceIfNotPresent(Collection* const collection, Args&&... args) {
  return collection->emplace(std::forward<Args>(args)...).second;
}

// Emplaces the given key-value pair into the collection. Returns true if the
// given key didn't previously exist. If the given key already existed in the
// map, its value is changed to the given "value" and false is returned.
template <class Collection>
bool EmplaceOrUpdate(
    Collection* const collection,
    const typename Collection::key_type& key,
    typename Collection::mapped_type&& value) {
  using mapped_type = typename Collection::mapped_type;
  auto it = collection->find(key);
  if (it == collection->end()) {
    collection->emplace(key, std::forward<mapped_type>(value));
    return true;
  }
  it->second = std::forward<mapped_type>(value);
  return false;
}

template <class Collection, class... Args>
void EmplaceOrDie(Collection* const collection, Args&&... args) {
  CHECK(EmplaceIfNotPresent(collection, std::forward<Args>(args)...))
      << "duplicate value";
}

//
// Lookup*()
//

// Looks up a given key and value pair in a collection and inserts the key-value
// pair if it's not already present. Returns a reference to the value associated
// with the key.
template <class Collection>
typename Collection::mapped_type& LookupOrInsert(
    Collection* const collection,
    const typename Collection::value_type& vt) {
  return collection->insert(vt).first->second;
}

// Same as above except the key-value are passed separately.
template <class Collection>
typename Collection::mapped_type& LookupOrInsert(
    Collection* const collection,
    const typename Collection::key_type& key,
    const typename Collection::mapped_type& value) {
  return LookupOrInsert(
      collection, typename Collection::value_type(key, value));
}

// It's similar to LookupOrInsert() but uses the emplace and r-value mechanics
// to achieve the desired results. The constructor of the new element is called
// with exactly the same arguments as supplied to emplace, forwarded via
// std::forward<Args>(args). The element may be constructed even if there
// already is an element with the same key in the container, in which case the
// newly constructed element will be destroyed immediately.
// For details, see
//   https://en.cppreference.com/w/cpp/container/map/emplace
//   https://en.cppreference.com/w/cpp/container/unordered_map/emplace
template <class Collection, class... Args>
typename Collection::mapped_type& LookupOrEmplace(
    Collection* const collection,
    Args&&... args) {
  return collection->emplace(std::forward<Args>(args)...).first->second;
}

//
// Misc Utility Functions
//

// Erases the collection item identified by the given key, and returns the value
// associated with that key. It is assumed that the value (i.e., the
// mapped_type) is a pointer. Returns NULL if the key was not found in the
// collection.
//
// Examples:
//   map<string, MyType*> my_map;
//
// One line cleanup:
//     delete EraseKeyReturnValuePtr(&my_map, "abc");
//
// Use returned value:
//     unique_ptr<MyType> value_ptr(EraseKeyReturnValuePtr(&my_map, "abc"));
//     if (value_ptr.get())
//       value_ptr->DoSomething();
//
// Note: if 'collection' is a multimap, this will only erase and return the
// first value.
template <class Collection>
typename Collection::mapped_type EraseKeyReturnValuePtr(
    Collection* const collection,
    const typename Collection::key_type& key) {
  auto it = collection->find(key);
  if (it == collection->end()) {
    return typename Collection::mapped_type();
  }
  typename Collection::mapped_type v = std::move(it->second);
  collection->erase(it);
  return v;
}

// Inserts all the keys from map_container into key_container, which must
// support insert(MapContainer::key_type).
//
// Note: any initial contents of the key_container are not cleared.
template <class MapContainer, class KeyContainer>
void InsertKeysFromMap(
    const MapContainer& map_container,
    KeyContainer* key_container) {
  CHECK(key_container != nullptr);
  for (typename MapContainer::const_iterator it = map_container.begin();
       it != map_container.end();
       ++it) {
    key_container->insert(it->first);
  }
}

// Appends all the keys from map_container into key_container, which must
// support push_back(MapContainer::key_type).
//
// Note: any initial contents of the key_container are not cleared.
template <class MapContainer, class KeyContainer>
void AppendKeysFromMap(
    const MapContainer& map_container,
    KeyContainer* key_container) {
  CHECK(key_container != nullptr);
  for (typename MapContainer::const_iterator it = map_container.begin();
       it != map_container.end();
       ++it) {
    key_container->push_back(it->first);
  }
}

// A more specialized overload of AppendKeysFromMap to optimize reallocations
// for the common case in which we're appending keys to a vector and hence can
// (and sometimes should) call reserve() first.
//
// (It would be possible to play SFINAE games to call reserve() for any
// container that supports it, but this seems to get us 99% of what we need
// without the complexity of a SFINAE-based solution.)
template <class MapContainer, class KeyType>
void AppendKeysFromMap(
    const MapContainer& map_container,
    std::vector<KeyType>* key_container) {
  CHECK(key_container != nullptr);
  // We now have the opportunity to call reserve(). Calling reserve() every
  // time is a bad idea for some use cases: libstdc++'s implementation of
  // vector<>::reserve() resizes the vector's backing store to exactly the
  // given size (unless it's already at least that big). Because of this,
  // the use case that involves appending a lot of small maps (total size
  // N) one by one to a vector would be O(N^2). But never calling reserve()
  // loses the opportunity to improve the use case of adding from a large
  // map to an empty vector (this improves performance by up to 33%). A
  // number of heuristics are possible; see the discussion in
  // cl/34081696. Here we use the simplest one.
  if (key_container->empty()) {
    key_container->reserve(map_container.size());
  }
  for (typename MapContainer::const_iterator it = map_container.begin();
       it != map_container.end();
       ++it) {
    key_container->push_back(it->first);
  }
}

// Inserts all the values from map_container into value_container, which must
// support push_back(MapContainer::mapped_type).
//
// Note: any initial contents of the value_container are not cleared.
template <class MapContainer, class ValueContainer>
void AppendValuesFromMap(
    const MapContainer& map_container,
    ValueContainer* value_container) {
  CHECK(value_container != nullptr);
  for (typename MapContainer::const_iterator it = map_container.begin();
       it != map_container.end();
       ++it) {
    value_container->push_back(it->second);
  }
}

// A more specialized overload of AppendValuesFromMap to optimize reallocations
// for the common case in which we're appending values to a vector and hence
// can (and sometimes should) call reserve() first.
//
// (It would be possible to play SFINAE games to call reserve() for any
// container that supports it, but this seems to get us 99% of what we need
// without the complexity of a SFINAE-based solution.)
template <class MapContainer, class ValueType>
void AppendValuesFromMap(
    const MapContainer& map_container,
    std::vector<ValueType>* value_container) {
  CHECK(value_container != nullptr);
  // See AppendKeysFromMap for why this is done.
  if (value_container->empty()) {
    value_container->reserve(map_container.size());
  }
  for (const auto& entry : map_container) {
    value_container->push_back(entry.second);
  }
}

// Compute and insert new value if it's absent from the map. Return a pair with
// a reference to the value and a bool indicating whether it was absent at
// first.
//
// This inspired on a similar java construct (url split in two lines):
// https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/ConcurrentHashMap.html
// #computeIfAbsent-K-java.util.function.Function
//
// It takes a reference to the key and a lambda function. If the key exists in
// the map, returns a pair with a pointer to the current value and 'false'. If
// the key does not exist in the map, it uses the lambda function to create a
// value, inserts it into the map, and returns a pair with a pointer to the new
// value and 'true'.
//
// Example usage:
//
// auto result = ComputeIfAbsentReturnAbsense(&my_collection,
//                                            my_key,
//                                            [] { return new_value; });
// MyValue* const value = result.first;
// if (result.second) ....
//
template <class MapContainer, typename Function>
std::pair<typename MapContainer::mapped_type* const, bool>
ComputeIfAbsentReturnAbsense(
    MapContainer* container,
    const typename MapContainer::key_type& key,
    Function compute_func) {
  typename MapContainer::iterator iter = container->find(key);
  bool new_value = iter == container->end();
  if (new_value) {
    std::pair<typename MapContainer::iterator, bool> result =
        container->emplace(key, compute_func());
    DCHECK(result.second) << "duplicate key: " << key;
    iter = result.first;
  }
  return std::make_pair(&iter->second, new_value);
}

// Like the above but doesn't return a pair, just returns a pointer to the
// value. Example usage:
//
// MyValue* const value = ComputeIfAbsent(&my_collection,
//                                        my_key,
//                                        [] { return new_value; });
//
template <class MapContainer, typename Function>
typename MapContainer::mapped_type* const ComputeIfAbsent(
    MapContainer* container,
    const typename MapContainer::key_type& key,
    Function compute_func) {
  return ComputeIfAbsentReturnAbsense(container, key, compute_func).first;
}
