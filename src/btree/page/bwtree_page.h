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
   * @param row
   * @param opts
   * @return Status
   */
  Status InsertRow(const property::Row &row, const Options &opts) noexcept;

  /**
   * @brief
   * Update a row in page.
   * note that sort key couldn't be changed.
   * if user want to update sort key, then delete followed by insert is
   * preferred.
   * @param tuple
   * @param opts
   * @return Status
   */
  Status UpdateRow(const property::Row &row, const Options &opts) noexcept;

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
   * @param row_ref
   * @return Status
   */
  Status GetRow(property::SortKeysRef sort_key, const Options &opts,
                RowRef *ref) const noexcept;

private:
  // TODO(sheep): replace ptr_mu to atomic_shared_ptr
  mutable ArcanedbLock ptr_mu_;
  std::shared_ptr<DeltaNode> ptr_;
};

} // namespace btree
} // namespace arcanedb