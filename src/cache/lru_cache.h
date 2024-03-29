// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#pragma once

#include <cassert>
#include <cstdint>
#include <cstring>
#include <memory>
#include <mutex>
#include <vector>

#include "absl/hash/hash.h"
#include "cache/cache.h"

namespace arcanedb {
namespace cache {

// LRU cache implementation
//
// Cache entries have an "in_cache" boolean indicating whether the cache has a
// reference on the entry.  The only ways that this can become false without the
// entry being passed to its "deleter" are via Erase(), via Insert() when
// an element with a duplicate key is inserted, or on destruction of the cache.
//
// The cache keeps two linked lists of items in the cache.  All items in the
// cache are in one list or the other, and never both.  Items still referenced
// by clients but erased from the cache are in neither list.  The lists are:
// - in-use:  contains the items currently referenced by clients, in no
//   particular order.  (This list is used for invariant checking.  If we
//   removed the check, elements that would otherwise be on this list could be
//   left as disconnected singleton lists.)
// - LRU:  contains the items not currently referenced by clients, in LRU order
// Elements are moved between these lists by the Ref() and Unref() methods,
// when they detect an element in the cache acquiring or losing its only
// external reference.

// An entry is a variable length heap-allocated structure.  Entries
// are kept in a circular doubly linked list ordered by access time.
struct LRUHandle {
  void *value;
  void (*deleter)(const std::string_view &, void *value);
  LRUHandle *next_hash;
  LRUHandle *next;
  LRUHandle *prev;
  size_t charge; // TODO(opt): Only allow uint32_t?
  size_t key_length;
  bool in_cache;    // Whether entry is in the cache.
  uint32_t refs;    // References, including cache reference, if present.
  uint32_t hash;    // Hash of key(); used for fast sharding and comparisons
  char key_data[1]; // Beginning of key

  std::string_view key() const {
    // next is only equal to this if the LRU handle is the list head of an
    // empty list. List heads never have meaningful keys.
    assert(next != this);

    return {key_data, key_length};
  }
};

// We provide our own simple hash table since it removes a whole bunch
// of porting hacks and is also faster than some of the built-in hash
// table implementations in some of the compiler/runtime combinations
// we have tested.  E.g., readrandom speeds up by ~5% over the g++
// 4.4.3's builtin hashtable.
class HandleTable {
public:
  HandleTable() { Resize(); }
  ~HandleTable() { delete[] list_; }

  LRUHandle *Lookup(const std::string_view &key, uint32_t hash) {
    return *FindPointer(key, hash);
  }

  LRUHandle *Insert(LRUHandle *h) {
    LRUHandle **ptr = FindPointer(h->key(), h->hash);
    LRUHandle *old = *ptr;
    h->next_hash = (old == nullptr ? nullptr : old->next_hash);
    *ptr = h;
    if (old == nullptr) {
      ++elems_;
      if (elems_ > length_) {
        // Since each cache entry is fairly large, we aim for a small
        // average linked list length (<= 1).
        Resize();
      }
    }
    return old;
  }

  LRUHandle *Remove(const std::string_view &key, uint32_t hash) {
    LRUHandle **ptr = FindPointer(key, hash);
    LRUHandle *result = *ptr;
    if (result != nullptr) {
      *ptr = result->next_hash;
      --elems_;
    }
    return result;
  }

private:
  // The table consists of an array of buckets where each bucket is
  // a linked list of cache entries that hash into the bucket.
  uint32_t length_{0};
  uint32_t elems_{0};
  LRUHandle **list_{nullptr};

  // Return a pointer to slot that points to a cache entry that
  // matches key/hash.  If there is no such cache entry, return a
  // pointer to the trailing slot in the corresponding linked list.
  LRUHandle **FindPointer(const std::string_view &key, uint32_t hash) {
    LRUHandle **ptr = &list_[hash & (length_ - 1)];
    while (*ptr != nullptr && ((*ptr)->hash != hash || key != (*ptr)->key())) {
      ptr = &(*ptr)->next_hash;
    }
    return ptr;
  }

  void Resize() {
    uint32_t new_length = 4;
    while (new_length < elems_) {
      new_length *= 2;
    }
    auto *new_list = new LRUHandle *[new_length];
    memset(new_list, 0, sizeof(new_list[0]) * new_length);
    uint32_t count = 0;
    for (uint32_t i = 0; i < length_; i++) {
      LRUHandle *h = list_[i];
      while (h != nullptr) {
        LRUHandle *next = h->next_hash;
        uint32_t hash = h->hash;
        auto *ptr = &new_list[hash & (new_length - 1)];
        h->next_hash = *ptr;
        *ptr = h;
        h = next;
        count++;
      }
    }
    assert(elems_ == count);
    delete[] list_;
    list_ = new_list;
    length_ = new_length;
  }
};

// A single shard of sharded cache.
template <typename Mutex> class LRUCache {
public:
  LRUCache();
  ~LRUCache();

  // Separate from constructor so caller can easily make an array of LRUCache
  void SetCapacity(size_t capacity) { capacity_ = capacity; }

  // Like Cache methods, but with an extra "hash" parameter.
  LRUHandle *Insert(const std::string_view &key, uint32_t hash, void *value,
                    size_t charge,
                    void (*deleter)(const std::string_view &key, void *value));
  LRUHandle *Lookup(const std::string_view &key, uint32_t hash);
  void Ref(LRUHandle *e);
  void Fork(LRUHandle *handle);
  void Release(LRUHandle *handle);
  void Prune();

  size_t TotalCharge() {
    std::lock_guard<decltype(mutex_)> lock(mutex_);
    return usage_;
  }
  void UpdateCharge(LRUHandle *handle, size_t new_charge) {
    std::lock_guard<decltype(mutex_)> lock(mutex_);
    usage_ = usage_ - handle->charge + new_charge;
    handle->charge = new_charge;
  }

private:
  void LRU_Remove(LRUHandle *e);
  void LRU_Append(LRUHandle *list, LRUHandle *e);
  void Unref(LRUHandle *e);
  bool FinishErase(LRUHandle *e);
  template <class StopFunc> void DoPrune(StopFunc &&stop_func);

  // Initialized before use.
  size_t capacity_{0};

  // mutex_ protects the following state.
  Mutex mutex_;
  size_t usage_{0};

  // Dummy head of LRU list.
  // lru.prev is newest entry, lru.next is oldest entry.
  // Entries have refs==1 and in_cache==true.
  LRUHandle lru_{};

  // Dummy head of in-use list.
  // Entries are in use by clients, and have refs >= 2 and in_cache==true.
  LRUHandle in_use_{};

  HandleTable table_{};
};

template <typename Mutex> inline LRUCache<Mutex>::LRUCache() {
  // Make empty circular linked lists.
  lru_.next = &lru_;
  lru_.prev = &lru_;
  in_use_.next = &in_use_;
  in_use_.prev = &in_use_;
}

template <typename Mutex> inline LRUCache<Mutex>::~LRUCache() {
  assert(in_use_.next == &in_use_); // Error if caller has an unreleased handle
  for (LRUHandle *e = lru_.next; e != &lru_;) {
    LRUHandle *next = e->next;
    assert(e->in_cache);
    e->in_cache = false;
    assert(e->refs == 1); // Invariant of lru_ list.
    Unref(e);
    e = next;
  }
}

template <typename Mutex> inline void LRUCache<Mutex>::Ref(LRUHandle *e) {
  if (e->refs == 1 && e->in_cache) { // If on lru_ list, move to in_use_ list.
    LRU_Remove(e);
    LRU_Append(&in_use_, e);
  }
  e->refs++;
}

template <typename Mutex> inline void LRUCache<Mutex>::Unref(LRUHandle *e) {
  assert(e->refs > 0);
  e->refs--;
  if (e->refs == 0) { // Deallocate.
    assert(!e->in_cache);
    (*e->deleter)(e->key(), e->value);
    free(e);
  } else if (e->in_cache && e->refs == 1) {
    // No longer in use; move to lru_ list.
    LRU_Remove(e);
    LRU_Append(&lru_, e);
  }
}

template <typename Mutex>
inline void LRUCache<Mutex>::LRU_Remove(LRUHandle *e) {
  e->next->prev = e->prev;
  e->prev->next = e->next;
}

template <typename Mutex>
inline void LRUCache<Mutex>::LRU_Append(LRUHandle *list, LRUHandle *e) {
  // Make "e" newest entry by inserting just before *list
  e->next = list;
  e->prev = list->prev;
  e->prev->next = e;
  e->next->prev = e;
}

template <typename Mutex>
inline LRUHandle *LRUCache<Mutex>::Lookup(const std::string_view &key,
                                          uint32_t hash) {
  std::lock_guard<decltype(mutex_)> lock(mutex_);
  LRUHandle *e = table_.Lookup(key, hash);
  if (e != nullptr) {
    Ref(e);
  }
  return e;
}

template <typename Mutex>
inline void LRUCache<Mutex>::Release(LRUHandle *handle) {
  std::lock_guard<decltype(mutex_)> lock(mutex_);
  Unref(handle);
}

template <typename Mutex> inline void LRUCache<Mutex>::Fork(LRUHandle *handle) {
  std::lock_guard<decltype(mutex_)> lock(mutex_);
  Ref(handle);
}

template <typename Mutex>
inline LRUHandle *LRUCache<Mutex>::Insert(
    const std::string_view &key, uint32_t hash, void *value, size_t charge,
    void (*deleter)(const std::string_view &key, void *value)) {
  std::lock_guard<decltype(mutex_)> lock(mutex_);

  auto *e =
      reinterpret_cast<LRUHandle *>(malloc(sizeof(LRUHandle) - 1 + key.size()));
  e->value = value;
  e->deleter = deleter;
  e->charge = charge;
  e->key_length = key.size();
  e->hash = hash;
  e->in_cache = false;
  e->refs = 1; // for the returned handle.
  std::memcpy(e->key_data, key.data(), key.size());

  if (capacity_ > 0) {
    e->refs++; // for the cache's reference.
    e->in_cache = true;
    LRU_Append(&in_use_, e);
    usage_ += charge;
    FinishErase(table_.Insert(e));
  } else { // don't cache. (capacity_==0 is supported and turns off caching.)
    // next is read by key() in an assert, so it must be initialized
    e->next = nullptr;
  }

  if (usage_ > capacity_) {
    DoPrune([&]() { return usage_ <= capacity_; });
  }

  return e;
}

// If e != nullptr, finish removing *e from the cache; it has already been
// removed from the hash table.  Return whether e != nullptr.
template <typename Mutex>
inline bool LRUCache<Mutex>::FinishErase(LRUHandle *e) {
  if (e != nullptr) {
    assert(e->in_cache);
    LRU_Remove(e);
    e->in_cache = false;
    usage_ -= e->charge;
    Unref(e);
  }
  return e != nullptr;
}

template <typename Mutex> inline void LRUCache<Mutex>::Prune() {
  std::lock_guard<decltype(mutex_)> lock(mutex_);
  return DoPrune([]() { return false; });
}

template <typename Mutex>
template <class StopFunc>
inline void LRUCache<Mutex>::DoPrune(StopFunc &&stop_func) {
  while (lru_.next != &lru_) {
    LRUHandle *e = lru_.next;
    assert(e->refs == 1);
    bool erased = FinishErase(table_.Remove(e->key(), e->hash));
    if (!erased) { // to avoid unused variable when compiled NDEBUG
      assert(erased);
    }

    if (stop_func()) {
      break;
    }
  }
}

template <typename Mutex> class ShardedLRUCache : public Cache {
private:
  const uint32_t num_shard_bits_;
  const uint32_t num_shards_;
  std::vector<LRUCache<Mutex>> shard_;
  Mutex id_mutex_;
  uint64_t last_id_;

  static inline uint32_t HashSlice(const std::string_view &s) {
    return absl::Hash<std::string_view>()(s);
  }

  // Note, hash >> 32 yields hash in gcc, not the zero we expect!
  uint32_t Shard(uint32_t hash) {
    return uint64_t(hash) >> (32 - num_shard_bits_);
  }

public:
  explicit ShardedLRUCache(size_t capacity, uint32_t num_shard_bits)
      : num_shard_bits_(num_shard_bits), num_shards_(1 << num_shard_bits_),
        shard_(num_shards_), last_id_(0) {
    const size_t per_shard = (capacity + (num_shards_ - 1)) / num_shards_;
    for (auto &s : shard_) {
      s.SetCapacity(per_shard);
    }
  }
  ~ShardedLRUCache() override = default;
  Handle *DoInsert(const std::string_view &key, void *value, size_t charge,
                   void (*deleter)(const std::string_view &key,
                                   void *value)) override {
    const uint32_t hash = HashSlice(key);
    return reinterpret_cast<Handle *>(
        shard_[Shard(hash)].Insert(key, hash, value, charge, deleter));
  }
  Handle *DoLookup(const std::string_view &key) override {
    const uint32_t hash = HashSlice(key);
    return reinterpret_cast<Handle *>(shard_[Shard(hash)].Lookup(key, hash));
  }
  void Fork(Handle *handle) override {
    auto *h = reinterpret_cast<LRUHandle *>(handle);
    shard_[Shard(h->hash)].Fork(reinterpret_cast<LRUHandle *>(handle));
  }
  void Release(Handle *handle) override {
    auto *h = reinterpret_cast<LRUHandle *>(handle);
    shard_[Shard(h->hash)].Release(reinterpret_cast<LRUHandle *>(handle));
  }
  void *Value(Handle *handle) override {
    return reinterpret_cast<LRUHandle *>(handle)->value;
  }
  uint64_t NewId() override {
    std::lock_guard<decltype(id_mutex_)> lock(id_mutex_);
    return ++(last_id_);
  }
  void Prune() override {
    for (auto &s : shard_) {
      s.Prune();
    }
  }
  size_t TotalCharge() override {
    size_t total = 0;
    for (auto &s : shard_) {
      total += s.TotalCharge();
    }
    return total;
  }

  void UpdateCharge(Handle *handle, size_t charge) override {
    auto *h = reinterpret_cast<LRUHandle *>(handle);
    shard_[Shard(h->hash)].UpdateCharge(reinterpret_cast<LRUHandle *>(handle),
                                        charge);
  }
};

template <typename Mutex = std::mutex>
inline std::unique_ptr<Cache> NewLRUCache(size_t capacity,
                                          uint32_t num_shard_bits = 4) {
  return std::make_unique<ShardedLRUCache<Mutex>>(capacity, num_shard_bits);
}

} // namespace cache
} // namespace arcanedb
