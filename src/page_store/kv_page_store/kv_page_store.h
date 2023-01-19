/**
 * @file kv_page_store.h
 * @author sheep (ysj1173886760@gmail.com)
 * @brief
 * @version 0.1
 * @date 2022-12-20
 *
 * @copyright Copyright (c) 2022
 *
 */

#pragma once

#include "common/macros.h"
#include "common/type.h"
#include "kv_store/leveldb_store.h"
#include "page_store/kv_page_store/index_page.h"
#include "page_store/page_store.h"
#include "util/thread_pool.h"
#include <cstdint>

namespace arcanedb {
namespace page_store {

class KvPageStore : public PageStore {
  enum class StoreType : uint8_t {
    IndexStore = 0,
    BaseStore = 1,
    DeltaStore = 2,
    StoreNum = 3
  };

public:
  static Status Open(const std::string &name, const Options &options,
                     std::shared_ptr<PageStore> *page_store) noexcept;

  static Status Destory(const std::string &name) noexcept;

  ~KvPageStore() override = default;

  /**
   * @brief
   * Update base page.
   * note that this will clear all delta pages.
   * @param page_id
   * @param options
   * @param data
   * @return Status
   */
  Status UpdateReplacement(const PageIdType &page_id,
                           const WriteOptions &options,
                           const std::string_view &data) noexcept override;

  /**
   * @brief
   * Prepend a delta.
   * @param page_id
   * @param options
   * @param data
   * @return Status
   */
  Status UpdateDelta(const PageIdType &page_id, const WriteOptions &options,
                     const std::string_view &data) noexcept override;

  /**
   * @brief
   * Delete a page, including base and delta.
   * @param page_id
   * @param options
   * @return Status
   */
  Status DeletePage(const PageIdType &page_id,
                    const WriteOptions &options) noexcept override;

  /**
   * @brief
   * Read a page.
   * pages will contains all physical pages corresponding to that page_id.
   * @param page_id
   * @param options
   * @param[out] pages
   * @return Status
   */
  Status ReadPage(const PageIdType &page_id, const ReadOptions &options,
                  std::vector<RawPage> *pages) noexcept override;

private:
  static std::string MakeStoreName_(const std::string &name, StoreType type) {
    switch (type) {
    case StoreType::IndexStore:
      return name + "::index_store";
    case StoreType::BaseStore:
      return name + "::base_store";
    case StoreType::DeltaStore:
      return name + "::delta_store";
    default:
      break;
    }
    UNREACHABLE();
    return "";
  }

  leveldb_store::AsyncLevelDB *GetIndexStore_() noexcept {
    return stores_[static_cast<uint8_t>(StoreType::IndexStore)].get();
  }
  leveldb_store::AsyncLevelDB *GetBaseStore_() noexcept {
    return stores_[static_cast<uint8_t>(StoreType::BaseStore)].get();
  }
  leveldb_store::AsyncLevelDB *GetDeltaStore_() noexcept {
    return stores_[static_cast<uint8_t>(StoreType::DeltaStore)].get();
  }
  leveldb_store::AsyncLevelDB *GetStoreBasedOnPageType(PageType type) noexcept {
    switch (type) {
    case PageType::BasePage:
      return GetBaseStore_();
    case PageType::DeltaPage:
      return GetDeltaStore_();
    default:
      break;
    }
    UNREACHABLE();
    return nullptr;
  }

  Status ReadIndexPage_(const PageIdType &page_id, IndexPage *index_page,
                        bool create_if_missing) noexcept;

  Status WriteIndexPage_(const PageIdType &page_id,
                         const IndexPage &index_page) noexcept;

  Status
  UpdateHelper_(const PageIdType &page_id, const std::string_view &data,
                std::function<PageIdType(IndexPage *)> new_page_id_generator,
                leveldb_store::AsyncLevelDB *store) noexcept;

  std::array<std::shared_ptr<leveldb_store::AsyncLevelDB>,
             static_cast<size_t>(StoreType::StoreNum)>
      stores_{nullptr};
  std::string name;
};

} // namespace page_store
} // namespace arcanedb