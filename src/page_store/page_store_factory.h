/**
 * @file page_store_factory.h
 * @author sheep (ysj1173886760@gmail.com)
 * @brief
 * @version 0.1
 * @date 2022-12-11
 *
 * @copyright Copyright (c) 2022
 *
 */

#pragma once

#include "page_store/page_store_if.h"

namespace arcanedb {
namespace page_store {

class PageStoreFactory {
public:
  /**
   * @brief Create a Page Store
   *
   * @param[in] name
   * @param[in] options
   * @param[out] page_store pointer to that page store
   * @return Status
   */
  static Status
  CreatePageStore(const std::string &name, const OpenOptions &options,
                  std::shared_ptr<PageStoreIf> *page_store) noexcept {
    return Status::Ok();
  }
};

} // namespace page_store
} // namespace arcanedb
