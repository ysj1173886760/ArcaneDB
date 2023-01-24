/**
 * @file versioned_btree.h
 * @author sheep (ysj1173886760@gmail.com)
 * @brief
 * @version 0.1
 * @date 2023-01-24
 *
 * @copyright Copyright (c) 2023
 *
 */

#pragma once

#include "btree/btree.h"
#include "btree/page/versioned_btree_page.h"

namespace arcanedb {
namespace btree {

class VersionedBtree {
public:
  explicit VersionedBtree(VersionedBtreePage *root_page) noexcept
      : root_page_(root_page) {}

  /**
   * @brief
   * Insert a row into page.
   * if row with same sortkey is existed, then we will overwrite that row.
   * i.e. semantic is to always perform upsert.
   * @param row
   * @param write_ts
   * @param opts
   * @return Status
   */
  Status SetRow(const property::Row &row, TxnTs write_ts,
                const Options &opts) noexcept;

  /**
   * @brief
   * Delete a row from page
   * @param sort_key
   * @param write_ts
   * @param opts
   * @return Status
   */
  Status DeleteRow(property::SortKeysRef sort_key, TxnTs write_ts,
                   const Options &opts) noexcept;

  /**
   * @brief
   * Get a row from page
   * @param tuple logical tuple that stores SortKey.
   * @param read_ts
   * @param opts
   * @param view
   * @return Status: Ok when row has been found
   *                 NotFound.
   */
  Status GetRow(property::SortKeysRef sort_key, TxnTs read_ts,
                const Options &opts, RowView *view) const noexcept;

  // TODO(sheep): support SMO interface

private:
  Status GetRowMultilevel_(property::SortKeysRef sort_key, TxnTs read_ts,
                           const Options &opts, RowView *view) const noexcept;

  VersionedBtreePage *root_page_{nullptr};
};

} // namespace btree
} // namespace arcanedb