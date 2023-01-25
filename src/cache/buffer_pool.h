/**
 * @file buffer_pool.h
 * @author sheep (ysj1173886760@gmail.com)
 * @brief
 * @version 0.1
 * @date 2023-01-25
 *
 * @copyright Copyright (c) 2023
 *
 */

#pragma once

#include "cache/cache.h"
#include "common/config.h"
#include "common/status.h"
#include "common/type.h"
#include <type_traits>

namespace arcanedb {
namespace cache {

/**
 * @brief
 * Wrapper for cache
 */
class BufferPool {
public:
  BufferPool() noexcept : lru_(common::Config::kCacheShardNum) {}

  /**
   * @brief
   * Get the page.
   * @tparam T
   * @param page_id
   * @return Result<CacheEntry *>
   */
  template <typename T>
  Status GetPage(const std::string &page_id, T **page) noexcept {
    static_assert(std::is_base_of<CacheEntry, T>::value,
                  "T must inherit from CacheEntry");
    auto alloc = [](const std::string_view &key,
                    std::unique_ptr<CacheEntry> *entry) -> Status {
      // TODO(sheep): load from store
      *entry = std::make_unique<T>(key);
      return Status::Ok();
    };
    auto result = lru_.GetEntry(page_id, alloc);
    if (likely(result.ok())) {
      // sheep: really don't want to use dynamic cast.
      *page = reinterpret_cast<T *>(result.GetValue());
    }
    return result.ToStatus();
  }

private:
  ShardedLRU lru_;
};

} // namespace cache
} // namespace arcanedb