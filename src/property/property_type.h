/**
 * @file property_type.h
 * @author sheep (ysj1173886760@gmail.com)
 * @brief
 * @version 0.1
 * @date 2023-01-07
 *
 * @copyright Copyright (c) 2023
 *
 */

#pragma once

#include "absl/container/inlined_vector.h"
#include <cstdint>
#include <string_view>
#include <unordered_map>
#include <variant>

namespace arcanedb {
namespace property {

using SchemaId = uint32_t;
using ColumnId = uint32_t;

constexpr size_t kDefaultColumnNum = 8;

enum class ValueType : uint8_t {
  Int32 = 0,
  Int64 = 1,
  Float = 2,
  Double = 3,
  String = 4,
  Bool = 5,
  Null = 6,
};

using Value =
    std::variant<int32_t, int64_t, float, double, std::string_view, bool>;
using OwnedValue =
    std::variant<int32_t, int64_t, float, double, std::string, bool>;

using ValueRefMap = std::unordered_map<ColumnId, Value>;

using ValueRefVec = absl::InlinedVector<Value, kDefaultColumnNum>;

// TODO(sheep): support default value and null value

struct Column {
  ColumnId column_id;
  std::string name;
  ValueType type;
};

struct RawSchema {
  absl::InlinedVector<Column, kDefaultColumnNum> columns;
  SchemaId schema_id;
  size_t sort_key_count;
};

struct ValueResult {
  Value value;
  std::unique_ptr<std::string> owned_str{};
};

} // namespace property
} // namespace arcanedb