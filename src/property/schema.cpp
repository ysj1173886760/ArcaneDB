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

namespace arcanedb {
namespace property {

Schema::Schema(const RawSchema &raw_schema) noexcept
    : schema_id_(raw_schema.schema_id), columns_(raw_schema.columns) {
  BuildColumnIndex_();
}

void Schema::BuildColumnIndex_() noexcept {
  for (size_t i = 0; i < columns_.size(); i++) {
    column_index_[columns_[i].column_id] = i;
  }
  CHECK(column_index_.size() == columns_.size());
}

Column *Schema::GetColumnRefById(ColumnId column_id) noexcept {
  auto it = column_index_.find(column_id);
  CHECK(it != column_index_.end());
  return &columns_[it->second];
}

Column *Schema::GetColumnRefByIndex(size_t index) noexcept {
  CHECK(index < columns_.size());
  return &columns_[index];
}

} // namespace property
} // namespace arcanedb