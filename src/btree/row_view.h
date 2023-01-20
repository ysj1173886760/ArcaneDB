/**
 * @file row_view.h
 * @author sheep (ysj1173886760@gmail.com)
 * @brief
 * @version 0.1
 * @date 2023-01-19
 *
 * @copyright Copyright (c) 2023
 *
 */

#pragma once

#include "property/row/row.h"
#include <functional>
#include <memory>
#include <vector>

namespace arcanedb {
namespace btree {

/**
 * @brief
 * Owner base
 */
class RowOwner : public std::enable_shared_from_this<RowOwner> {
public:
  virtual ~RowOwner() noexcept {}
};

/**
 * @brief
 * row view with owner ship
 */
class RowView : public property::RowConcept<RowView> {
public:
  /**
   * @brief byte copy the row ptr
   *
   * @param row
   */
  explicit RowView(const property::Row &row) : row_(row) {}

  RowView() = default;

  Status GetProp(property::ColumnId id, property::ValueResult *value,
                 property::Schema *schema) const noexcept {
    return row_.GetProp(id, value, schema);
  }

  property::SortKeysRef GetSortKeys() const noexcept {
    return row_.GetSortKeys();
  }

  void SetOwner(std::shared_ptr<const RowOwner> owner) noexcept {
    owner_ = std::move(owner);
  }

  const property::Row &GetRow() const noexcept { return row_; }

private:
  property::Row row_{};
  std::shared_ptr<const RowOwner> owner_{};
};

} // namespace btree
} // namespace arcanedb