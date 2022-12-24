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

#include "kv_store/leveldb_store.h"
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
  static Status Open(const std::string &name, const OpenOptions &options,
                     std::shared_ptr<PageStore> *page_store) noexcept;

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
  static std::string MakeStoreName(const std::string &name, StoreType type) {
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
    // FIXME: unreachable
    return "";
  }

  std::array<std::shared_ptr<leveldb_store::AsyncLevelDB>,
             static_cast<size_t>(StoreType::StoreNum)>
      stores_{nullptr};
  std::string name;
};

} // namespace page_store
} // namespace arcanedb