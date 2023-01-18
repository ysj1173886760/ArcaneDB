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

#include "btree/page/page_concept.h"
#include "common/type.h"
#include "btree/page/delta_node.h"

namespace arcanedb {
namespace btree {

class BwTreePage : public PageConcept<BwTreePage> {
public:
  /**
   * @brief
   * Insert a row into page
   * @param row
   * @param schema
   * @return Status
   */
  Status InsertRow(const property::Row &row,
                   const property::Schema *schema) noexcept;

  /**
   * @brief
   * Update a row in page.
   * note that sort key couldn't be changed.
   * if user want to update sort key, then delete followed by insert is
   * preferred.
   * @param tuple
   * @param schema
   * @return Status
   */
  Status UpdateRow(const property::Row &row,
                   const property::Schema *schema) noexcept;

  /**
   * @brief
   * Delete a row from page
   * @param sort_key
   * @param schema
   * @return Status
   */
  Status DeleteRow(property::SortKeysRef sort_key,
                   const property::Schema *schema) noexcept;

private:
  // TODO(sheep): replace ptr_mu to atomic_shared_ptr
  ArcanedbLock ptr_mu_;
  std::shared_ptr<DeltaNode> ptr_;
};

} // namespace btree
} // namespace arcanedb