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

#include "btree/page/versioned_btree_page.h"
#include "cache/cache.h"
#include "cache/lru_cache.h"
#include "common/config.h"
#include "common/status.h"
#include "common/type.h"
#include "util/singleflight.h"
#include <type_traits>

namespace arcanedb {
namespace cache {

/**
 * @brief
 * Wrapper for cache
 */
class BufferPool {
public:
  BufferPool() noexcept
      : cache_(
            NewLRUCache<bthread::Mutex>(common::Config::kCacheCapacity,
                                        common::Config::kCacheShardNumBits)) {}

  ~BufferPool() noexcept { ARCANEDB_INFO("Buffer pool destory"); }

  // simple wrapper
  class PageHolder {
  public:
    btree::VersionedBtreePage *operator->() const noexcept {
      return handle_holder_.TValue<btree::VersionedBtreePage>();
    }

    explicit PageHolder(Cache::HandleHolder handle_holder) noexcept
        : handle_holder_(std::move(handle_holder)) {}

    PageHolder() = default;
    PageHolder(const PageHolder &) = default;
    PageHolder &operator=(const PageHolder &) = default;
    PageHolder(PageHolder &&) = default;
    PageHolder &operator=(PageHolder &&) = default;

  private:
    Cache::HandleHolder handle_holder_{};
  };

  /**
   * @brief Get the Page
   *
   * @param page_id
   * @param page_handle
   * @return Status
   */
  Status GetPage(const std::string_view &page_id,
                 PageHolder *page_handle) noexcept {
    auto handle_holder = cache_->Lookup(page_id);
    if (handle_holder) {
      *page_handle = PageHolder(std::move(handle_holder));
      return Status::Ok();
    }
    auto s = load_group_.Do(
        page_id, &handle_holder,
        [&](const std::string_view &key, Cache::HandleHolder *val) {
          auto page = std::make_unique<btree::VersionedBtreePage>(key);
          // TODO(sheep): deserialize from page store.
          auto handle = cache_->Insert(
              key, page.get(), sizeof(btree::VersionedBtreePage), &PageDeleter);
          *val = std::move(handle);
          page.release();
          return Status::Ok();
        });

    *page_handle = PageHolder(std::move(handle_holder));
    return s;
  }

  static void PageDeleter(const std::string_view &key, void *value) noexcept {
    delete static_cast<btree::VersionedBtreePage *>(value);
  }

private:
  std::unique_ptr<Cache> cache_;
  util::SingleFlight<Cache::HandleHolder, std::string_view> load_group_;
};

} // namespace cache
} // namespace arcanedb