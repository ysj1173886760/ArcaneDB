/**
 * @file kv_page_store.cpp
 * @author sheep (ysj1173886760@gmail.com)
 * @brief
 * @version 0.1
 * @date 2022-12-20
 *
 * @copyright Copyright (c) 2022
 *
 */

#include "page_store/kv_page_store/kv_page_store.h"
#include "common/config.h"
#include "common/logger.h"
#include "kv_store/leveldb_store.h"
#include "page_store/kv_page_store/index_page.h"
#include "util/codec/buf_reader.h"
#include "util/codec/buf_writer.h"
#include "util/thread_pool.h"
#include <memory>

namespace arcanedb {
namespace page_store {

Status KvPageStore::Open(const std::string &name, const OpenOptions &options,
                         std::shared_ptr<PageStore> *page_store) noexcept {
  auto thread_pool = options.thread_pool;
  if (thread_pool == nullptr) {
    // create if missing
    thread_pool = std::make_shared<util::ThreadPool>(
        common::Config::ThreadPoolDefaultNum);
  }
  auto open = [&](const std::string &db_name) {
    std::shared_ptr<leveldb_store::AsyncLevelDB> db;
    leveldb_store::Options options;
    auto s =
        leveldb_store::AsyncLevelDB::Open(db_name, &db, thread_pool, options);
    if (!s.ok()) {
      return Result<std::shared_ptr<leveldb_store::AsyncLevelDB>>::Err();
    }
    return Result<std::shared_ptr<leveldb_store::AsyncLevelDB>>(std::move(db));
  };

  auto result = std::make_shared<KvPageStore>();
  result->name = name;

  for (int i = 0; i < static_cast<int>(StoreType::StoreNum); i++) {
    auto store = open(MakeStoreName_(name, static_cast<StoreType>(i)));
    if (!store.ok()) {
      return Status::Err();
    }
    result->stores_[i] = std::move(store).GetValue();
  }

  *page_store = result;
  return Status::Ok();
}

Status KvPageStore::Destory(const std::string &name) noexcept {
  for (int i = 0; i < static_cast<int>(StoreType::StoreNum); i++) {
    auto store_name = MakeStoreName_(name, static_cast<StoreType>(i));
    auto s = leveldb_store::AsyncLevelDB::DestroyDB(store_name);
    if (!s.ok()) {
      return s;
    }
  }
  return Status::Ok();
}

Status KvPageStore::ReadIndexPage_(const PageIdType &page_id,
                                   IndexPage *index_page,
                                   bool create_if_missing) noexcept {
  auto *index_store = GetIndexStore_();
  std::string buffer;
  auto s = index_store->Get(page_id, &buffer);
  if (!s.ok()) {
    if (create_if_missing && s.IsNotFound()) {
      *index_page = IndexPage(page_id);
      return Status::Ok();
    }
    return s;
  }
  *index_page = IndexPage(page_id);
  util::BufReader reader(buffer);
  return index_page->DeserializationFrom(&reader);
}

Status KvPageStore::WriteIndexPage_(const PageIdType &page_id,
                                    const IndexPage &index_page) noexcept {
  util::BufWriter writer;
  index_page.SerializationTo(&writer);
  auto bytes = writer.Detach();
  auto *index_store = GetIndexStore_();
  return index_store->Put(page_id, bytes);
}

Status KvPageStore::UpdateHelper_(
    const PageIdType &page_id, const std::string_view &data,
    std::function<PageIdType(IndexPage *)> new_page_id_generator,
    leveldb_store::AsyncLevelDB *store) noexcept {
  // first read index page
  IndexPage index_page;
  auto s = ReadIndexPage_(page_id, &index_page, true /*create if missing*/);
  if (!s.ok()) {
    return s;
  }
  // generate new page id
  auto new_page_id = new_page_id_generator(&index_page);
  s = store->Put(new_page_id, data);
  if (!s.ok()) {
    return s;
  }
  auto *index_store = GetIndexStore_();
  return WriteIndexPage_(page_id, index_page);
}

Status KvPageStore::UpdateReplacement(const PageIdType &page_id,
                                      const WriteOptions &options,
                                      const std::string_view &data) noexcept {
  // TODO: delete stale delta pages
  return UpdateHelper_(
      page_id, data,
      [](IndexPage *index_page) { return index_page->UpdateReplacement(); },
      GetBaseStore_());
}

Status KvPageStore::UpdateDelta(const PageIdType &page_id,
                                const WriteOptions &options,
                                const std::string_view &data) noexcept {
  return UpdateHelper_(
      page_id, data,
      [](IndexPage *index_page) { return index_page->UpdateDelta(); },
      GetDeltaStore_());
}

// TODO: fork join here.
Status KvPageStore::DeletePage(const PageIdType &page_id,
                               const WriteOptions &options) noexcept {
  IndexPage index_page;
  auto s = ReadIndexPage_(page_id, &index_page, false /*create if missing*/);
  if (!s.ok()) {
    if (s.IsNotFound()) {
      // already deleted.
      return Status::Ok();
    }
    return s;
  }
  auto pages = index_page.ListAllPhysicalPages();
  for (const auto &page : pages) {
    auto *store = GetStoreBasedOnPageType(page.type);
    s = store->Delete(page.page_id);
    if (!s.ok()) {
      return s;
    }
  }
  auto *index_store = GetIndexStore_();
  return index_store->Delete(page_id);
}

Status KvPageStore::ReadPage(const PageIdType &page_id,
                             const ReadOptions &options,
                             std::vector<RawPage> *pages) noexcept {
  pages->clear();
  IndexPage index_page;
  auto s = ReadIndexPage_(page_id, &index_page, false /*create if missing*/);
  if (!s.ok()) {
    return s;
  }
  auto physical_pages = index_page.ListAllPhysicalPages();
  pages->reserve(physical_pages.size());
  for (int i = 0; i < physical_pages.size(); i++) {
    auto *store = GetStoreBasedOnPageType(physical_pages[i].type);
    std::string bytes;
    s = store->Get(physical_pages[i].page_id, &bytes);
    if (s.ok()) {
      pages->emplace_back(
          RawPage{.type = physical_pages[i].type, .binary = std::move(bytes)});
    } else {
      if (s.IsNotFound()) {
        LOG_WARN("PageNotFound, PageId: %s", page_id.c_str());
      } else {
        // skip not found, and regard it as empty page.
        return s;
      }
    }
  }
  return Status::Ok();
}

} // namespace page_store
} // namespace arcanedb