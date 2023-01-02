#pragma once

#include "absl/container/flat_hash_map.h"
#include "bthread/mutex.h"
#include "butil/macros.h"
#include "common/status.h"
#include <list>
#include <memory>
#include <string>
#include <unordered_map>

namespace arcanedb {
namespace cache {

class LruMap;

/**
 * @brief
 * Manipulating CacheEntry requires external synchronization.
 */
class CacheEntry {
public:
  explicit CacheEntry(const std::string &key) : key_(key) {}

  virtual ~CacheEntry() = default;

  void SetDirty() noexcept {
    std::lock_guard<bthread::Mutex> guard(mu_);
    is_dirty_ = true;
  }

  void UnsetDirty() noexcept {
    std::lock_guard<bthread::Mutex> guard(mu_);
    is_dirty_ = false;
  }

  bool IsDirty() const noexcept {
    std::lock_guard<bthread::Mutex> guard(mu_);
    return is_dirty_;
  }

  std::string_view GetKey() const noexcept { return key_; }

private:
  friend class LruMap;

  const std::string key_;
  mutable bthread::Mutex mu_;
  bool is_dirty_{false}; // guarded by mu_
  typename std::list<std::shared_ptr<CacheEntry>>::iterator
      iter_; // guarded by mu_
};

class ShardedLRU {
public:
  using AllocFunc = std::function<Status(const std::string &key,
                                         std::shared_ptr<CacheEntry> *entry)>;
  explicit ShardedLRU(size_t shard_num) : shard_num_(shard_num) {}

  Status Init() noexcept;

private:
  size_t shard_num_;
};

class LruMap {
public:
  using AllocFunc = ShardedLRU::AllocFunc;
  Status GetOrAllocEntry(const std::string &key,
                         std::shared_ptr<CacheEntry> *entry,
                         AllocFunc alloc) noexcept;
  Status GetEntry(const std::string &key,
                  std::shared_ptr<CacheEntry> *entry) noexcept;

private:
  DISALLOW_COPY_AND_ASSIGN(LruMap);

  Status AllocEntry_(const std::string &key, std::shared_ptr<CacheEntry> *entry,
                     const AllocFunc &alloc) noexcept;

  std::shared_ptr<CacheEntry>
  GetFromMap_(const std::string &key) const noexcept;

  void InsertIntoMap_(const std::shared_ptr<CacheEntry> &entry) noexcept;

  void InsertIntoList_(const std::shared_ptr<CacheEntry> &entry) noexcept;
  /**
   * @brief
   * delete entry from map. note that this entry must be deleted from lru list
   * first.
   * @param entry
   * @return true delete succeed
   * @return false when entry doesn't exist in map
   */
  bool DelFromMap_(const std::shared_ptr<CacheEntry> &entry) noexcept;

  /**
   * @brief
   * delete entry from list.
   * @param entry
   * @return true when delete succeed
   * @return false when entry doesn't exist in list
   */
  bool DelFromList_(const std::shared_ptr<CacheEntry> &entry) noexcept;

  mutable bthread::Mutex map_mu_;
  mutable bthread::Mutex list_mu_;
  std::list<std::shared_ptr<CacheEntry>> list_; // LRU list
  absl::flat_hash_map<std::string_view, std::shared_ptr<CacheEntry>> map_;
};

} // namespace cache
} // namespace arcanedb