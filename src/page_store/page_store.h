/**
 * @file page_store_if.h
 * @author sheep (ysj1173886760@gmail.com)
 * @brief page store interface
 * @version 0.1
 * @date 2022-12-11
 *
 * @copyright Copyright (c) 2022
 *
 */

#pragma once

#include "common/status.h"
#include "common/type.h"
#include "page_store/options.h"
#include <string>
#include <string_view>

namespace arcanedb {
namespace page_store {

/**
 * @brief
 * Interface according to LLAMA.
 */
class PageStore {
public:
  enum class PageType : uint8_t { BasePage, DeltaPage };

  struct RawPage {
    PageType type;
    std::string binary;
  };

  struct PageIdAndType {
    PageIdType page_id;
    PageType type;
  };

  /**
   * @brief Open a Page Store
   *
   * @param[in] name
   * @param[in] options
   * @param[out] page_store pointer to that page store
   * @return Status
   */
  static Status Open(const std::string &name, const Options &options,
                     std::shared_ptr<PageStore> *page_store) noexcept {
    return Status::Ok();
  }

  /**
   * @brief
   * Update base page.
   * note that this will clear all delta pages.
   * @param page_id
   * @param options
   * @param data
   * @return Status
   */
  virtual Status UpdateReplacement(const PageIdType &page_id,
                                   const WriteOptions &options,
                                   const std::string_view &data) noexcept = 0;

  /**
   * @brief
   * Prepend a delta.
   * @param page_id
   * @param options
   * @param data
   * @return Status
   */
  virtual Status UpdateDelta(const PageIdType &page_id,
                             const WriteOptions &options,
                             const std::string_view &data) noexcept = 0;

  /**
   * @brief
   * Delete a page, including base and delta.
   * @param page_id
   * @param options
   * @return Status
   */
  virtual Status DeletePage(const PageIdType &page_id,
                            const WriteOptions &options) noexcept = 0;

  /**
   * @brief
   * Read a page.
   * pages will contains all physical pages corresponding to that page_id.
   * @param page_id
   * @param options
   * @param[out] pages
   * @return Status
   */
  virtual Status ReadPage(const PageIdType &page_id, const ReadOptions &options,
                          std::vector<RawPage> *pages) noexcept = 0;
};

} // namespace page_store
} // namespace arcanedb