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
namespace page_store {
class PageStore;
}

namespace cache {

class Flusher;

/**
 * @brief
 * Wrapper for cache
 */
class BufferPool {
public:
  // page_store == nullptr indicates that we don't needs to flush dirty pages
  BufferPool(std::shared_ptr<page_store::PageStore> page_store) noexcept;

  ~BufferPool() noexcept;

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

    void UpdateCharge(size_t charge) { handle_holder_.UpdateCharge(charge); }

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
                 PageHolder *page_handle) noexcept;

  void TryInsertDirtyPage(const PageHolder &page_holder) noexcept;

  void Prune() noexcept { cache_->Prune(); }

  size_t TotalCharge() noexcept { return cache_->TotalCharge(); }

  void ForceFlushAllPages() noexcept;

private:
  static void PageDeleter(const std::string_view &key, void *value) noexcept {
    delete static_cast<btree::VersionedBtreePage *>(value);
  }

  std::unique_ptr<Cache> cache_;
  std::shared_ptr<page_store::PageStore> page_store_{};
  std::shared_ptr<Flusher> flusher_{};
  util::SingleFlight<Cache::HandleHolder, std::string_view> load_group_;
};

} // namespace cache
} // namespace arcanedb