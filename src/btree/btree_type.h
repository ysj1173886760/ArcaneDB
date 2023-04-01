/**
 * @file btree_type.h
 * @author sheep (ysj1173886760@gmail.com)
 * @brief
 * @version 0.1
 * @date 2023-01-19
 *
 * @copyright Copyright (c) 2023
 *
 */

#pragma once

#include "common/type.h"
#include "property/row/row.h"
#include "util/view.h"
#include <cstdint>
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

class RowRef : property::RowConcept<RowRef> {
public:
  RowRef(const property::Row row) noexcept : row_(row) {}

  const property::Row &GetRow() const noexcept { return row_; }

  operator const property::Row &() const { return row_; }

  Status GetProp(property::ColumnId id, property::ValueResult *value,
                 const property::Schema *schema) const noexcept {
    return row_.GetProp(id, value, schema);
  }

  // TODO(sheep): impl GetProps

  property::SortKeysRef GetSortKeys() const noexcept {
    return row_.GetSortKeys();
  }

  RowRef(const RowRef &rhs) = default;
  RowRef &operator=(const RowRef &rhs) = default;

private:
  property::Row row_;
};

class RowWithTs : public RowRef {
public:
  RowWithTs(const property::Row row, TxnTs ts) noexcept
      : RowRef(row), ts_(ts) {}

  RowWithTs(const property::Row row) noexcept : RowRef(row), ts_(0) {}

  TxnTs GetTs() const noexcept { return ts_; }

private:
  TxnTs ts_{};
};

using RowView = util::Views<RowWithTs, RowOwner>;

using RangeScanRowView = util::Views<RowRef, RowOwner>;

enum class PageType : uint8_t {
  InternalPage,
  LeafPage,
};

} // namespace btree
} // namespace arcanedb