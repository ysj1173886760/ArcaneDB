/**
 * @file internal_page.h
 * @author sheep (ysj1173886760@gmail.com)
 * @brief
 * @version 0.1
 * @date 2023-01-22
 *
 * @copyright Copyright (c) 2023
 *
 */

#pragma once

#include "btree/options.h"
#include "common/status.h"
#include "common/type.h"
#include "property/sort_key/sort_key.h"

namespace arcanedb {
namespace btree {

class InternalPage {
public:
  InternalPage() = default;

  /**
   * @brief
   * Get page id though sort_key
   * @param opts
   * @param sort_key
   * @param page_id
   * @return Status
   */
  Status GetPageId(const Options &opts, property::SortKeysRef sort_key,
                   PageIdType *page_id) const noexcept;

private:
};

} // namespace btree
} // namespace arcanedb