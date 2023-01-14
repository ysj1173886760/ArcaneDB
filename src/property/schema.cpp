/**
 * @file schema.cc
 * @author sheep (ysj1173886760@gmail.com)
 * @brief
 * @version 0.1
 * @date 2023-01-08
 *
 * @copyright Copyright (c) 2023
 *
 */

#include "property/schema.h"
#include "butil/logging.h"
#include "common/macros.h"
#include "property/property_type.h"

namespace arcanedb {
namespace property {

Schema::Schema(const RawSchema &raw_schema) noexcept
    : schema_id_(raw_schema.schema_id), columns_(raw_schema.columns),
      sort_key_count_(raw_schema.sort_key_count) {
  BuildColumnIndex_();
  BuildOffsetCacheForSimpleRow_();
  BuildOffsetCacheForRow_();
}

void Schema::BuildColumnIndex_() noexcept {
  for (size_t i = 0; i < columns_.size(); i++) {
    column_index_[columns_[i].column_id] = i;
  }
  CHECK(column_index_.size() == columns_.size());
}

const Column *Schema::GetColumnRefById(ColumnId column_id) const noexcept {
  auto it = column_index_.find(column_id);
  CHECK(it != column_index_.end());
  return &columns_[it->second];
}

const Column *Schema::GetColumnRefByIndex(size_t index) const noexcept {
  CHECK(index < columns_.size());
  return &columns_[index];
}

size_t Schema::GetColumnIndex(ColumnId column_id) const noexcept {
  auto it = column_index_.find(column_id);
  CHECK(it != column_index_.end());
  return it->second;
}

size_t Schema::GetColumnOffsetForSimpleRow(size_t index) const noexcept {
  CHECK(index <= columns_.size());
  return offset_cache_simple_row_[index];
}

void Schema::BuildOffsetCacheForSimpleRow_() noexcept {
  offset_cache_simple_row_.resize(columns_.size() + 1);
  size_t offset = 0;
  for (size_t i = 0; i < columns_.size(); i++) {
    auto &column = columns_[i];
    offset_cache_simple_row_[i] = offset;
    switch (column.type) {
    case ValueType::Int32:
    case ValueType::Float:
      offset += 4;
      break;
    case ValueType::Int64:
    case ValueType::Double:
    case ValueType::String:
      offset += 8;
      break;
    case ValueType::Bool:
      offset += 1;
      break;
    default:
      UNREACHABLE();
    }
  }
  offset_cache_simple_row_.back() = offset;
}

void Schema::BuildOffsetCacheForRow_() noexcept {
  offset_cache_row_.resize(columns_.size() + 1);
  size_t offset = 0;
  for (size_t i = sort_key_count_; i < columns_.size(); i++) {
    auto &column = columns_[i];
    offset_cache_row_[i] = offset;
    switch (column.type) {
    case ValueType::Int32:
    case ValueType::Float:
    case ValueType::String:
      offset += 4;
      break;
    case ValueType::Int64:
    case ValueType::Double:
      offset += 8;
      break;
    case ValueType::Bool:
      offset += 1;
      break;
    default:
      UNREACHABLE();
    }
  }
  offset_cache_row_.back() = offset;
}

size_t Schema::GetColumnOffsetForRow(size_t index) const noexcept {
  CHECK(index <= columns_.size() && index >= sort_key_count_);
  return offset_cache_row_[index];
}

} // namespace property
} // namespace arcanedb