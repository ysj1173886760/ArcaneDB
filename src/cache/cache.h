/**
 * @file cache.h
 * @author sheep (ysj1173886760@gmail.com)
 * @brief
 * @version 0.1
 * @date 2023-01-02
 *
 * @copyright Copyright (c) 2023
 *
 */
#pragma once

#include "absl/container/flat_hash_map.h"
#include "bthread/mutex.h"
#include "butil/macros.h"
#include "common/config.h"
#include "common/status.h"
#include "util/singleflight.h"
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

  explicit CacheEntry(std::string_view key) : key_(std::string(key)) {}

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

  void Ref() noexcept {
    std::lock_guard<bthread::Mutex> guard(mu_);
    ref_cnt_ += 1;
  }

  void Unref() noexcept {
    std::lock_guard<bthread::Mutex> guard(mu_);
    ref_cnt_ -= 1;
  }

  std::string_view GetKey() const noexcept { return key_; }

private:
  friend class LruMap;

  const std::string key_;
  typename std::list<CacheEntry *>::iterator iter_{}; // guarded by mu_
  mutable bthread::Mutex mu_;
  bool is_dirty_{false}; // guarded by mu_
  bool in_cache_{false}; // guarded by mu_
  int32_t ref_cnt_{1};   // guarded by mu_
};

class ShardedLRU {
public:
  using AllocFunc = std::function<Status(const std::string_view &key,
                                         std::unique_ptr<CacheEntry> *entry)>;
  explicit ShardedLRU(size_t shard_num)
      : shard_num_(shard_num), shards_(shard_num) {}

  // Status Init() noexcept;

  /**
   * @brief Get CacheEntry.
   * alloc could be nullptr when user don't want to perform IO when cache
   * missed.
   * @param key
   * @param alloc
   * @return Result<CacheEntry *>
   */
  Result<CacheEntry *> GetEntry(const std::string &key,
                                AllocFunc alloc) noexcept;

private:
  size_t shard_num_;
  std::vector<LruMap> shards_;
};

class LruMap {
public:
  using AllocFunc = ShardedLRU::AllocFunc;

  Result<CacheEntry *> GetEntry(const std::string &key,
                                AllocFunc alloc) noexcept;

  LruMap() = default;

private:
  DISALLOW_COPY_AND_ASSIGN(LruMap);

  FRIEND_TEST(CacheTest, LruMapTest);
  FRIEND_TEST(CacheTest, ConcurrentLruMapTest);

  /**
   * @brief
   * Check all ref cnt is 1. i.e. held by cache
   * @return true
   * @return false
   */
  bool TEST_CheckAllRefCnt() noexcept;

  /**
   * @brief
   * Check all entry is undirty.
   * @return true
   * @return false
   */
  bool TEST_CheckAllIsUndirty() noexcept;

  Status AllocEntry_(const std::string_view &key, CacheEntry **entry,
                     const AllocFunc &alloc) noexcept;

  void RefreshEntry(CacheEntry *entry) noexcept;

  CacheEntry *GetFromMap_(const std::string_view &key) const noexcept;

  void InsertIntoMap_(std::unique_ptr<CacheEntry> entry) noexcept;

  void InsertIntoList_(CacheEntry *entry) noexcept;

  bool DelFromMap_(const CacheEntry &entry) noexcept;

  bool DelFromList_(const CacheEntry &entry) noexcept;

  mutable bthread::Mutex list_mu_;
  /**
   * @brief
   * LRU list, end is newest
   */
  std::list<CacheEntry *> list_;

  // map holds one refcnt of cache entry
  mutable bthread::Mutex map_mu_;
  absl::flat_hash_map<std::string_view, std::unique_ptr<CacheEntry>> map_;

  util::SingleFlight<CacheEntry> single_flight_;
};

} // namespace cache
} // namespace arcanedb