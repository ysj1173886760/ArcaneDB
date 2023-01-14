/**
 * @file schema.h
 * @author sheep (ysj1173886760@gmail.com)
 * @brief
 * @version 0.1
 * @date 2023-01-07
 *
 * @copyright Copyright (c) 2023
 *
 */

#pragma once

#include <string>

#include "absl/container/flat_hash_map.h"
#include "absl/container/inlined_vector.h"
#include "property/property_type.h"

namespace arcanedb {
namespace property {

class Schema {
public:
  Schema(const RawSchema &raw_schema) noexcept;

  const Column *GetColumnRefById(ColumnId column_id) const noexcept;

  const Column *GetColumnRefByIndex(size_t index) const noexcept;

  size_t GetColumnIndex(ColumnId column_id) const noexcept;

  SchemaId GetSchemaId() const noexcept { return schema_id_; }

  size_t GetColumnNum() const noexcept { return columns_.size(); }

  /**
   * @brief Get column offset.
   * if index == columns_.size(), then we will return total length of
   * fixed length area.
   * @param index
   * @return size_t
   */
  size_t GetColumnOffsetForSimpleRow(size_t index) const noexcept;

  size_t GetColumnOffsetForRow(size_t index) const noexcept;

  size_t GetSortKeyCount() const noexcept { return sort_key_count_; }

private:
  void BuildColumnIndex_() noexcept;

  void BuildOffsetCacheForSimpleRow_() noexcept;

  void BuildOffsetCacheForRow_() noexcept;

  absl::InlinedVector<Column, kDefaultColumnNum> columns_;
  SchemaId schema_id_;
  // mapping from column id to column index
  absl::flat_hash_map<ColumnId, size_t> column_index_;
  absl::InlinedVector<size_t, kDefaultColumnNum + 1> offset_cache_simple_row_;
  absl::InlinedVector<size_t, kDefaultColumnNum + 1> offset_cache_row_;

  // first "sort_key_count_" columns are sort key column
  size_t sort_key_count_;
};

} // namespace property
} // namespace arcanedb