/**
 * @file bwtree_page.h
 * @author sheep (ysj1173886760@gmail.com)
 * @brief
 * @version 0.1
 * @date 2023-01-08
 *
 * @copyright Copyright (c) 2023
 *
 */

// !! deprecated
// !! redirect to versioned_bwtree_page

#pragma once

#include "btree/page/delta_node.h"
#include "btree/page/page_concept.h"
#include "butil/containers/doubly_buffered_data.h"
#include "common/type.h"

namespace arcanedb {
namespace btree {

class BwTreePage : public PageConcept<BwTreePage> {
public:
  /**
   * @brief
   * Insert a row into page
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
   * @return Status
   */
  Status GetRow(property::SortKeysRef sort_key, const Options &opts,
                RowView *view) const noexcept;

  size_t TEST_GetDeltaLength() const noexcept {
    auto ptr = GetPtr_();
    return ptr->GetTotalLength();
  }

private:
  using DoublyBufferedData =
      butil::DoublyBufferedData<std::shared_ptr<DeltaNode>>;
  FRIEND_TEST(BwTreePageTest, CompactionTest);
  FRIEND_TEST(BwTreePageTest, ConcurrentCompactionTest);

  std::shared_ptr<DeltaNode> Compaction_(DeltaNode *current_ptr) noexcept;

  void MaybePerformCompaction_(const Options &opts,
                               DeltaNode *current_ptr) noexcept;

  std::shared_ptr<DeltaNode> GetPtr_() const noexcept {
    DoublyBufferedData::ScopedPtr scoped_ptr;
    CHECK(ptr_.Read(&scoped_ptr) == 0);
    return *scoped_ptr;
  }

  void UpdatePtr_(std::shared_ptr<DeltaNode> new_node) const noexcept {
    ptr_.Modify(UpdateFn_, new_node);
  }

  static bool UpdateFn_(std::shared_ptr<DeltaNode> &old_node,
                        std::shared_ptr<DeltaNode> new_node) {
    old_node = new_node;
    return true;
  }

  // TODO(sheep) use group commit to optimize write performance
  // mutable ArcanedbLock write_mu_{"BwTreePageWriteMutex"};
  mutable ArcanedbLock write_mu_;
  mutable DoublyBufferedData ptr_;
};

} // namespace btree
} // namespace arcanedb