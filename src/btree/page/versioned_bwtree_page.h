/**
 * @file versioned_bwtree_page.h
 * @author sheep (ysj1173886760@gmail.com)
 * @brief
 * @version 0.1
 * @date 2023-01-23
 *
 * @copyright Copyright (c) 2023
 *
 */

#pragma once

#include "btree/btree_type.h"
#include "btree/page/versioned_delta_node.h"
#include "butil/containers/doubly_buffered_data.h"
#include "common/options.h"
#include "common/status.h"
#include "property/row/row.h"

namespace arcanedb {
namespace btree {

class VersionedBwTreePage {
public:
  /**
   * @brief
   * Insert a row into page
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
   * @return Status
   */
  Status GetRow(property::SortKeysRef sort_key, TxnTs read_ts,
                const Options &opts, RowView *view) const noexcept;

  size_t TEST_GetDeltaLength() const noexcept {
    auto ptr = GetPtr_();
    return ptr->GetTotalLength();
  }

private:
  using DoublyBufferedData =
      butil::DoublyBufferedData<std::shared_ptr<VersionedDeltaNode>>;

  std::shared_ptr<VersionedDeltaNode>
  Compaction_(VersionedDeltaNode *current_ptr) noexcept;

  void MaybePerformCompaction_(const Options &opts,
                               VersionedDeltaNode *current_ptr) noexcept;

  std::shared_ptr<VersionedDeltaNode> GetPtr_() const noexcept {
    DoublyBufferedData::ScopedPtr scoped_ptr;
    CHECK(ptr_.Read(&scoped_ptr) == 0);
    return *scoped_ptr;
  }

  void UpdatePtr_(std::shared_ptr<VersionedDeltaNode> new_node) const noexcept {
    ptr_.Modify(UpdateFn_, new_node);
  }

  static bool UpdateFn_(std::shared_ptr<VersionedDeltaNode> &old_node,
                        std::shared_ptr<VersionedDeltaNode> new_node) {
    old_node = new_node;
    return true;
  }

  // TODO(sheep) use group commit to optimize write performance
  mutable ArcanedbLock write_mu_{"VersionedBwTreePageWriteMutex"};
  mutable DoublyBufferedData ptr_;
};

} // namespace btree
} // namespace arcanedb