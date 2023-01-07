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

#include "absl/container/inlined_vector.h"
#include "property/property_type.h"

namespace arcanedb {
namespace property {

// TODO(sheep): support default value and null value

struct Column {
  std::string name;
  ValueType type;
  ColumnId column_id;
};

constexpr size_t kDefaultColumnNum = 8;

struct Schema {
  absl::InlinedVector<Column, kDefaultColumnNum> columns;
  SchemaId schema_id;
};

} // namespace property
} // namespace arcanedb