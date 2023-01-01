#pragma once

#include "bthread/mutex.h"
#include "common/status.h"
#include <list>
#include <string>
#include <unordered_map>

namespace arcanedb {
namespace cache {

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

private:
  mutable bthread::Mutex mu_;
  bool is_dirty_{false};
  const std::string key_;
};

class CacheShard {
  mutable bthread::Mutex map_mu_;
  mutable bthread::Mutex list_mu_;
  std::list<std::shared_ptr<CacheEntry>> list_; // LRU list
  std::unordered_map<std::string_view, std::shared_ptr<CacheEntry>> map_;
};

class ShardedCache {
  explicit ShardedCache(size_t shard_num) : shard_num_(shard_num) {}

  Status Init() noexcept;

private:
  size_t shard_num_;
};

} // namespace cache
} // namespace arcanedb