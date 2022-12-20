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

#include "page_store/kv_page_store.h"
#include "common/config.h"
#include "kv_store/leveldb_store.h"
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
    auto s = leveldb_store::AsyncLevelDB::Open(name, &db, thread_pool, options);
    if (!s.ok()) {
      return Result<std::shared_ptr<leveldb_store::AsyncLevelDB>>::Err();
    }
    return Result<std::shared_ptr<leveldb_store::AsyncLevelDB>>(std::move(db));
  };

  auto result = std::make_shared<KvPageStore>();
  result->name = name;

  for (int i = 0; i < static_cast<int>(StoreType::StoreNum); i++) {
    auto store = open(MakeStoreName(name, static_cast<StoreType>(i)));
    if (!store.ok()) {
      return Status::Err();
    }
    result->stores_[i] = std::move(store).GetValue();
  }

  *page_store = result;
  return Status::Ok();
}

Status KvPageStore::UpdateReplacement(const PageIdType &page_id,
                                      const WriteOptions &options,
                                      const std::string_view &data) noexcept {
  return Status::Ok();
}

Status KvPageStore::UpdateDelta(const PageIdType &page_id,
                                const WriteOptions &options,
                                const std::string_view &data) noexcept {
  return Status::Ok();
}

Status KvPageStore::DeletePage(const PageIdType &page_id,
                               const WriteOptions &options) noexcept {
  return Status::Ok();
}

Status KvPageStore::ReadPage(const PageIdType &page_id,
                             const ReadOptions &options,
                             std::vector<RawPage> *pages) noexcept {
  return Status::Ok();
}

} // namespace page_store
} // namespace arcanedb