/**
 * @file page_concept.h
 * @author sheep (ysj1173886760@gmail.com)
 * @brief
 * @version 0.1
 * @date 2023-01-08
 *
 * @copyright Copyright (c) 2023
 *
 */

#include "common/status.h"
#include "handler/logical_tuple.h"
#include "property/row/row.h"
#include "property/schema.h"

namespace arcanedb {
namespace btree {

/**
 * @brief
 * Page concept of a single now in btree.
 * @tparam PageType
 */
template <typename PageType> class PageConcept {
public:
  /**
   * @brief
   * Insert a row into page
   * @param row
   * @param schema
   * @return Status
   */
  Status InsertRow(const property::Row &row,
                   const property::Schema *schema) noexcept {
    return Real()->InsertRow(row, schema);
  }

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
                   const property::Schema *schema) noexcept {
    return Real()->UpdateRow(row, schema);
  }

  /**
   * @brief
   * Delete a row from page
   * @param sort_key
   * @param schema
   * @return Status
   */
  Status DeleteRow(property::SortKeysRef sort_key,
                   const property::Schema *schema) noexcept {
    return Real()->DeleteRow(sort_key, schema);
  }

  // TODO(sheep): support filter
  /**
   * @brief
   * Get a row from page
   * @param tuple logical tuple that stores SortKey.
   * @param schema
   * @return Status
   */
  Status GetRow(const handler::LogicalTuple &tuple,
                const property::Schema *schema) noexcept {
    return Real()->GetRow(tuple, schema);
  }

  // TODO(sheep): support scan

  // TODO(sheep): split and merge

private:
  PageType *Real() noexcept { return static_cast<PageType *>(this); }
};

} // namespace btree
} // namespace arcanedb