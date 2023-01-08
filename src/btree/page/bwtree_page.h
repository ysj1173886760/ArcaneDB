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

namespace arcanedb {
namespace btree {

class BwTreePage : public PageConcept<BwTreePage> {
public:
  /**
   * @brief
   * Insert a row into page
   * @param tuple
   * @param schema
   * @return Status
   */
  Status InsertRow(const handler::LogicalTuple &tuple,
                   const property::Schema *schema) noexcept;

  /**
   * @brief
   * Update a row in page.
   * Behaviour of UpdateRow is more like delete a row then insert another one.
   * @param tuple
   * @param schema
   * @return Status
   */
  Status UpdateRow(const handler::LogicalTuple &tuple,
                   const property::Schema *schema) noexcept;

  /**
   * @brief
   * Delete a row from page
   * @param tuple tuple could only contains SortKey of current btree since we
   * don't need other properties to delete a row.
   * @param schema
   * @return Status
   */
  Status DeleteRow(const handler::LogicalTuple &tuple,
                   const property::Schema *schema) noexcept;

  /**
   * @brief
   * Get a row from page
   * @param tuple logical tuple that stores SortKey.
   * @param schema
   * @return Status
   */
  Status GetRow(const handler::LogicalTuple &tuple,
                const property::Schema *schema) noexcept;

private:
};

} // namespace btree
} // namespace arcanedb