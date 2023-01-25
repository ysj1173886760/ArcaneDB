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

  static BufferPool *GetInstance() noexcept {
    static BufferPool bpm;
    return &bpm;
  }

  /**
   * @brief
   * Get the page.
   * note that T must be default constructible.
   * @tparam T
   * @param page_id
   * @return Result<CacheEntry *>
   */
  template <typename T>
  Result<CacheEntry *> GetPage(const PageIdType &page_id) noexcept {
    static_assert(std::is_default_constructible<T>::value,
                  "T must be default constructible");
    auto alloc = [](const std::string_view &key,
                    std::unique_ptr<CacheEntry> *entry) -> Status {
      // TODO(sheep): load from store
      *entry = std::make_unique<T>();
      return Status::Ok();
    };
    return lru_.GetEntry(page_id, alloc);
  }

private:
  ShardedLRU lru_;
};

} // namespace cache
} // namespace arcanedb