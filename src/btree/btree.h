/**
 * @file btree.h
 * @author sheep (ysj1173886760@gmail.com)
 * @brief
 * @version 0.1
 * @date 2023-01-22
 *
 * @copyright Copyright (c) 2023
 *
 */

#pragma once

#include "btree/page/btree_page.h"

namespace arcanedb {
namespace btree {

class Btree {
public:
  explicit Btree(BtreePage *root_page) noexcept : root_page_(root_page) {}

  /**
   * @brief
   * Insert a row into page.
   * if row with same sortkey is existed, then we will overwrite that row.
   * i.e. semantic is to always perform upsert.
   * @param row
   * @param opts
   * @return Status
   */
  Status SetRow(const property::Row &row, const Options &opts) noexcept;

  /**
   * @brief
   * Delete a row from page
   * @param sort_key
   * @param opts
   * @return Status
   */
  Status DeleteRow(property::SortKeysRef sort_key,
                   const Options &opts) noexcept;

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
                RowView *view) const noexcept;

  // TODO(sheep): support SMO interface

private:
  Status GetRowMultilevel_(property::SortKeysRef sort_key, const Options &opts,
                           RowView *view) const noexcept;

  BtreePage *root_page_{nullptr};
};

} // namespace btree
} // namespace arcanedb