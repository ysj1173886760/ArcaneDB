/**
 * @file buffer_pool.cpp
 * @author sheep (ysj1173886760@gmail.com)
 * @brief
 * @version 0.1
 * @date 2023-03-25
 *
 * @copyright Copyright (c) 2023
 *
 */

#include "cache/buffer_pool.h"
#include "cache/flusher.h"
#include <cassert>

namespace arcanedb {
namespace cache {

BufferPool::BufferPool(
    std::shared_ptr<page_store::PageStore> page_store) noexcept
    : cache_(NewLRUCache<bthread::Mutex>(common::Config::kCacheCapacity,
                                         common::Config::kCacheShardNumBits)) {
  if (page_store) {
      page_store_ = std::move(page_store);
      flusher_ = std::make_shared<Flusher>(common::Config::kFlusherShardNum,
                                         page_store_);
  }
}

Status BufferPool::GetPage(const std::string_view &page_id,
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

        if (page_store_) {
          page_store::ReadOptions read_opts;
          std::vector<page_store::PageStore::RawPage> pages;
          auto s = page_store_->ReadPage(page->GetPageKeyRef(), read_opts, &pages);

          if (s.ok()) {
            assert(pages.size() == 1);
            s = page->Deserialize(pages[0].binary);
          } 
          if (!s.ok() && !s.IsNotFound()) {
            return s;
          }
        }

        auto handle = cache_->Insert(
            key, page.get(), sizeof(btree::VersionedBtreePage), &PageDeleter);
        *val = std::move(handle);
        page.release();
        return Status::Ok();
      });

  *page_handle = PageHolder(std::move(handle_holder));
  return s;
}

void BufferPool::TryInsertDirtyPage(const PageHolder &page_holder) noexcept {
  if (flusher_) {
    flusher_->TryInsertDirtyPage(page_holder);
  }
}

void BufferPool::ForceFlushAllPages() noexcept {
  if (flusher_) {
    flusher_->ForceFlushAllPages();
  }
}

} // namespace cache
} // namespace arcanedb