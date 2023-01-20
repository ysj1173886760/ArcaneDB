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
#pragma once

#include "btree/page/delta_node.h"
#include "btree/page/page_concept.h"
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
   * @param res
   * @return Status
   */
  Status GetRow(property::SortKeysRef sort_key, const Options &opts,
                property::Row *res) const noexcept;

private:
  FRIEND_TEST(BwTreePageTest, CompactionTest);

  std::shared_ptr<DeltaNode> Compaction_() noexcept;

  void MaybePerformCompaction_(const Options &opts) noexcept;

  // TODO(sheep): replace ptr_mu to atomic_shared_ptr
  mutable ArcanedbLock ptr_mu_{"BwTreePageMutex"};
  std::shared_ptr<DeltaNode> ptr_; // guarded by ptr_mu_
};

} // namespace btree
} // namespace arcanedb