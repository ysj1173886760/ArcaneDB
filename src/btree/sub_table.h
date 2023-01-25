/**
 * @file sub_table.h
 * @author sheep (ysj1173886760@gmail.com)
 * @brief
 * @version 0.1
 * @date 2023-01-25
 *
 * @copyright Copyright (c) 2023
 *
 */

#pragma once

#include "btree/versioned_btree.h"

namespace arcanedb {
namespace btree {

/**
 * @brief
 * Currently just a wrapper for btree.
 * For supporting secondary-index in the future.
 */
class SubTable {
public:
  SubTable() = default;

  /**
   * @brief
   *
   * @param table_key
   * @param opts
   * @param sub_table
   * @return Status
   */
  static Status OpenSubTable(const std::string_view &table_key,
                             const Options &opts,
                             std::unique_ptr<SubTable> *sub_table) noexcept;

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
   * @param tuple SortKey.
   * @param read_ts
   * @param opts
   * @param view
   * @return Status: Ok when row has been found
   *                 NotFound.
   */
  Status GetRow(property::SortKeysRef sort_key, TxnTs read_ts,
                const Options &opts, RowView *view) const noexcept;

private:
  VersionedBtree cluster_index;
};

} // namespace btree
} // namespace arcanedb