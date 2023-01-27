/**
 * @file page_concept.h
 * @author sheep (ysj1173886760@gmail.com)
 * @brief
 * @version 0.1
 * @date 2023-01-08
 *
 * @copyright Copyright (c) 2023
 *
 */

#pragma once

#include "btree/btree_type.h"
#include "common/options.h"
#include "common/status.h"
#include "property/row/row.h"
#include "property/schema.h"

namespace arcanedb {
namespace btree {

/**
 * @brief
 * Page concept of a single now in btree.
 * @tparam PageType
 */
template <typename PageType> class PageConcept {
public:
  /**
   * @brief
   * Insert a row into page.
   * if row with same sortkey is existed, then we will overwrite that row.
   * i.e. semantic is to always perform upsert.
   * @param row
   * @param opts
   * @return Status
   */
  Status SetRow(const property::Row &row, const Options &opts) noexcept {
    return Real()->SetRow(row, opts);
  }

  /**
   * @brief
   * Delete a row from page
   * @param sort_key
   * @param opts
   * @return Status
   */
  Status DeleteRow(property::SortKeysRef sort_key,
                   const Options &opts) noexcept {
    return Real()->DeleteRow(sort_key, opts);
  }

  // TODO(sheep): support filter
  /**
   * @brief
   * Get a row from page
   * @param tuple logical tuple that stores SortKey.
   * @param opts
   * @param view
   * @return Status: Ok when row has been found
   *                 NotFound.
   */
  Status GetRow(property::SortKeysRef sort_key, const Options &opts,
                RowView *view) const noexcept {
    return Real()->GetRow(sort_key, opts, view);
  }

  // TODO(sheep): support scan

  // TODO(sheep): split and merge

private:
  PageType *Real() noexcept { return static_cast<PageType *>(this); }
};

} // namespace btree
} // namespace arcanedb