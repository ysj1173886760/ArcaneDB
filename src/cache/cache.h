// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// A Cache is an interface that maps keys to values.  It has internal
// synchronization and may be safely accessed concurrently from
// multiple threads.  It may automatically evict entries to make room
// for new entries.  Values have a specified charge against the cache
// capacity.  For example, a cache where the values are variable
// length strings, may use the length of the string as the charge for
// the string.
//
// A builtin cache implementation with a least-recently-used eviction
// policy is provided.  Clients may use their own implementations if
// they want something more sophisticated (like scan-resistance, a
// custom eviction policy, variable cache sizing, etc.)

#pragma once
#include "common/macros.h"
#include <cstdint>
#include <memory>
#include <string_view>
#include <utility>

namespace arcanedb {
namespace cache {

class Cache {
public:
  Cache() = default;

  // Destroys all existing entries by calling the "deleter"
  // function that was passed to the constructor.
  virtual ~Cache() = default;

protected:
  // Opaque handle to an entry stored in the cache.
  struct Handle {};

public:
  /// Holder of opaque handle to an entry stored in the cache.
  class HandleHolder {
  public:
    HandleHolder() = default;
    ~HandleHolder() { Release(); }

    HandleHolder(HandleHolder &&rhs) noexcept { Move(std::move(rhs)); }

    HandleHolder &operator=(HandleHolder &&rhs) noexcept {
      Release();
      Move(std::move(rhs));
      return *this;
    }

    HandleHolder(const HandleHolder &rhs) noexcept
        : cache_(rhs.cache_), handle_(rhs.handle_), value_(rhs.value_) {
      if (rhs) {
        cache_->Fork(handle_);
      }
    }

    HandleHolder &operator=(const HandleHolder &rhs) noexcept {
      HandleHolder tmp(rhs);
      std::swap(cache_, tmp.cache_);
      std::swap(handle_, tmp.handle_);
      std::swap(value_, tmp.value_);
      return *this;
    }

    /// Return true if the stored handle is not null.
    explicit operator bool() const noexcept { return value_ != nullptr; }

    void Release() noexcept {
      if (handle_ != nullptr) {
        cache_->Release(handle_);
        handle_ = nullptr;
        cache_ = nullptr;
        value_ = nullptr;
      }
    }

    /// Return the value encapsulated
    template <class T> T *TValue() const noexcept {
      return static_cast<T *>(value_);
    }

    inline void UpdateCharge(size_t charge) noexcept {
      cache_->UpdateCharge(handle_, charge);
    }

  private:
    friend class Cache;

    HandleHolder(Cache *cache, Handle *handle, void *value)
        : cache_(cache), handle_(handle), value_(value) {}

    inline void Move(HandleHolder &&rhs) {
      cache_ = rhs.cache_;
      handle_ = rhs.handle_;
      value_ = rhs.value_;
      rhs.cache_ = nullptr;
      rhs.handle_ = nullptr;
      rhs.value_ = nullptr;
    }

  private:
    Cache *cache_{nullptr};
    Handle *handle_{nullptr};
    void *value_{nullptr};
  };

  // Insert a mapping from key->value into the cache and assign it
  // the specified charge against the total cache capacity.
  //
  // Returns a handle that corresponds to the mapping.
  // The caller must destroy handle holder when the returned mapping is no
  // longer needed.
  //
  // When the inserted entry is no longer needed, the key and
  // value will be passed to "deleter".
  HandleHolder Insert(const std::string_view &key, void *value, size_t charge,
                      void (*deleter)(const std::string_view &key,
                                      void *value)) {
    auto *handle = DoInsert(key, value, charge, deleter);
    return HandleHolder(this, handle, Value(handle));
  }

  // If the cache has no mapping for "key", returns a false handle holder.
  //
  // Else return a handle that corresponds to the mapping.
  // The caller must destroy handle holder when the returned mapping is no
  // longer needed.
  HandleHolder Lookup(const std::string_view &key) {
    auto *handle = DoLookup(key);
    if (handle == nullptr) {
      return HandleHolder(nullptr, nullptr, nullptr);
    }
    return HandleHolder(this, handle, Value(handle));
  }

  // Return a new numeric id.  May be used by multiple clients who are
  // sharing the same cache to partition the key space.  Typically the
  // client will allocate a new id at startup and prepend the id to
  // its cache keys.
  virtual uint64_t NewId() = 0;

  // Remove all cache entries that are not actively in use.  Memory-constrained
  // applications may wish to call this method to reduce memory usage.
  // Default implementation of Prune() does nothing.  Subclasses are strongly
  // encouraged to override the default implementation.  A future release of
  // leveldb may change Prune() to a pure abstract method.
  virtual void Prune() {}

  // Return an estimate of the combined charges of all elements stored in the
  // cache.
  virtual size_t TotalCharge() = 0;

protected:
  // Insert a mapping from key->value into the cache and assign it
  // the specified charge against the total cache capacity.
  //
  // Returns a handle that corresponds to the mapping.  The caller
  // must call this->Release(handle) when the returned mapping is no
  // longer needed.
  //
  // When the inserted entry is no longer needed, the key and
  // value will be passed to "deleter".
  virtual Handle *
  DoInsert(const std::string_view &key, void *value, size_t charge,
           void (*deleter)(const std::string_view &key, void *value)) = 0;

  // If the cache has no mapping for "key", returns nullptr.
  //
  // Else return a handle that corresponds to the mapping.  The caller
  // must call this->Release(handle) when the returned mapping is no
  // longer needed.
  virtual Handle *DoLookup(const std::string_view &key) = 0;

  // Increment the reference count of a handle.
  // Note that user should make Release call corresponding to every Ref call.
  // REQUIRES: handle must not have been released yet.
  // REQUIRES: handle must have been returned by a method on *this.
  virtual void Fork(Handle *handle) = 0;

  // Release a mapping returned by a previous Lookup().
  // REQUIRES: handle must not have been released yet.
  // REQUIRES: handle must have been returned by a method on *this.
  virtual void Release(Handle *handle) = 0;

  // Return the value encapsulated in a handle returned by a
  // successful Lookup().
  // REQUIRES: handle must not have been released yet.
  // REQUIRES: handle must have been returned by a method on *this.
  virtual void *Value(Handle *handle) = 0;

  virtual void UpdateCharge(Handle *handle, size_t charge) = 0;

private:
  DISALLOW_COPY_AND_ASSIGN(Cache);
};

} // namespace cache
} // namespace arcanedb
