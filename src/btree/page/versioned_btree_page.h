/**
 * @file versioned_btree_page.h
 * @author sheep (ysj1173886760@gmail.com)
 * @brief
 * @version 0.1
 * @date 2023-01-24
 *
 * @copyright Copyright (c) 2023
 *
 */

#pragma once

#include "btree/btree_type.h"
#include "btree/page/internal_page.h"
#include "btree/page/versioned_bwtree_page.h"

namespace arcanedb {
namespace btree {

/**
 * @brief
 * Real page type.
 * IndexPage could either be leaf page or internal page.
 */
class VersionedBtreePage {

public:
  /**
   * @brief
   * Default ctor, construct leaf page.
   */
  VersionedBtreePage() noexcept {
    leaf_page_ = std::make_unique<VersionedBwTreePage>();
    ModifyPageType(PageType::LeafPage);
  }

  /**
   * @brief
   * Meta interfaces
   */

  /**
   * @brief
   * Get page type
   * @return PageType
   */
  PageType GetPageType() const noexcept {
    return page_type_.load(std::memory_order_acquire);
  }

  /**
   * @brief
   * Modify page type
   * @param type
   */
  void ModifyPageType(PageType type) noexcept {
    page_type_.store(type, std::memory_order_release);
  }

  /**
   * @brief
   * Interfaces for leaf page type
   */

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
                const Options &opts) noexcept {
    assert(leaf_page_);
    return leaf_page_->SetRow(row, write_ts, opts);
  }

  /**
   * @brief
   * Delete a row from page
   * @param sort_key
   * @param write_ts
   * @param opts
   * @return Status
   */
  Status DeleteRow(property::SortKeysRef sort_key, TxnTs write_ts,
                   const Options &opts) noexcept {
    assert(leaf_page_);
    return leaf_page_->DeleteRow(sort_key, write_ts, opts);
  }

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
                const Options &opts, RowView *view) const noexcept {
    assert(leaf_page_);
    return leaf_page_->GetRow(sort_key, read_ts, opts, view);
  }

  /**
   * @brief
   * Interfaces for internal page type
   */

  /**
   * @brief
   * Get page id though sort_key
   * @param opts
   * @param sort_key
   * @param view
   * @return Status
   */
  Status GetPageId(const Options &opts, property::SortKeysRef sort_key,
                   InternalRowView *view) const noexcept {
    assert(internal_page_);
    return internal_page_->GetPageId(opts, sort_key, view);
  }

  /**
   * @brief
   * Inserting internal rows into internal page, corresponding to btree split
   * operation. If old_sort_key is not empty, we will use new_internal_rows to
   * replace the entry which has same sort_key with old_sort_key. otherwise, we
   * will use new_internal_rows to overwrite the entire internal page.
   * @param opts
   * @param old_sort_key
   * @param new_internal_rows
   * @return Status
   */
  Status Split(const Options &opts, property::SortKeysRef old_sort_key,
               std::vector<InternalRow> new_internal_rows) noexcept {
    assert(internal_page_);
    return internal_page_->Split(opts, old_sort_key,
                                 std::move(new_internal_rows));
  }

private:
  // TODO(sheep): introduce Page interface
  // for different page type
  std::unique_ptr<VersionedBwTreePage> leaf_page_;
  std::unique_ptr<InternalPage> internal_page_;
  std::atomic<PageType> page_type_;
};

} // namespace btree
} // namespace arcanedb