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

class RowWithTs : property::RowConcept<RowWithTs> {
public:
  RowWithTs(const property::Row row, TxnTs ts) noexcept : row_(row), ts_(ts) {}

  RowWithTs(const property::Row row) noexcept : row_(row), ts_(0) {}

  const property::Row &GetRow() const noexcept { return row_; }

  TxnTs GetTs() const noexcept { return ts_; }

  operator const property::Row &() const { return row_; }

  Status GetProp(property::ColumnId id, property::ValueResult *value,
                 property::Schema *schema) const noexcept {
    return row_.GetProp(id, value, schema);
  }

  // TODO(sheep): impl GetProps

  property::SortKeysRef GetSortKeys() const noexcept {
    return row_.GetSortKeys();
  }

private:
  const property::Row row_;
  TxnTs ts_;
};

using RowView = util::Views<RowWithTs, RowOwner>;

enum class PageType : uint8_t {
  InternalPage,
  LeafPage,
};

} // namespace btree
} // namespace arcanedb