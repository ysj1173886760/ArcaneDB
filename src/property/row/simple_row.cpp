/**
 * @file simple_row.cpp
 * @author sheep (ysj1173886760@gmail.com)
 * @brief
 * @version 0.1
 * @date 2023-01-08
 *
 * @copyright Copyright (c) 2023
 *
 */

#include "property/row/simple_row.h"
#include "absl/container/inlined_vector.h"
#include "butil/logging.h"
#include "common/macros.h"
#include "property/property_type.h"
#include "util/codec/buf_reader.h"
#include "util/codec/encoding.h"
#include <string_view>

namespace arcanedb {
namespace property {

Status SimpleRow::GetProp(ColumnId id, Value *value, Schema *schema) noexcept {
  DCHECK(ptr_ != nullptr);
  auto index = schema->GetColumnIndex(id);
  auto offset = schema->GetColumnOffsetForSimpleRow(index);
  auto type = schema->GetColumnRefByIndex(index)->type;
  std::string_view value_ref(ptr_ + offset, GetTypeLength_(type));
  switch (type) {
  case ValueType::Int32: {
    int32_t v;
    util::ReadBuf(value_ref.data(), &v);
    *value = v;
    break;
  }
  case ValueType::Int64: {
    int64_t v;
    util::ReadBuf(value_ref.data(), &v);
    *value = v;
    break;
  }
  case ValueType::Float: {
    float v;
    util::ReadBuf(value_ref.data(), &v);
    *value = v;
    break;
  }
  case ValueType::Double: {
    double v;
    util::ReadBuf(value_ref.data(), &v);
    *value = v;
    break;
  }
  case ValueType::Bool: {
    uint8_t v;
    util::ReadBuf(value_ref.data(), &v);
    *value = (v != 0);
    break;
  }
  case ValueType::String: {
    uint32_t string_offset;
    uint32_t string_length;
    util::ReadBuf(value_ref.data(), &string_offset);
    util::ReadBuf(value_ref.data() + 4, &string_length);
    *value = std::string_view(ptr_ + string_offset, string_length);
    break;
  }
  default:
    UNREACHABLE();
  }
  return Status::Ok();
}

Status SimpleRow::Serialize(const ValueRefVec &value_ref_vec,
                            util::BufWriter *buf_writer,
                            Schema *schema) noexcept {
  auto column_num = schema->GetColumnNum();
  CHECK(value_ref_vec.size() == column_num);
  // store index of va fields
  absl::InlinedVector<std::string_view, kDefaultColumnNum> va_fields;
  size_t string_offset = schema->GetColumnOffsetForSimpleRow(column_num);
  for (size_t i = 0; i < column_num; i++) {
    auto type = schema->GetColumnRefByIndex(i)->type;
    CHECK(static_cast<uint8_t>(type) == value_ref_vec[i].index());
    switch (type) {
    case ValueType::Int32:
    case ValueType::Int64:
    case ValueType::Float:
    case ValueType::Double:
    case ValueType::Bool: {
      buf_writer->WriteVariant(value_ref_vec[i]);
      break;
    }
    case ValueType::String: {
      auto data = std::get<std::string_view>(value_ref_vec[i]);
      uint32_t offset = string_offset;
      uint32_t length = data.size();
      va_fields.push_back(data);
      buf_writer->WriteBytes(offset);
      buf_writer->WriteBytes(length);
      string_offset += length;
      break;
    }
    }
  }
  // serialize string
  for (const auto &str : va_fields) {
    buf_writer->WriteBytes(str);
  }
  return Status::Ok();
}

Status SimpleRow::Serialize(const ValueRefMap &value_ref_map,
                            util::BufWriter *buf_writer,
                            Schema *schema) noexcept {
  ValueRefVec vec;
  for (size_t i = 0; i < schema->GetColumnNum(); i++) {
    auto it = value_ref_map.find(schema->GetColumnRefByIndex(i)->column_id);
    if (it == value_ref_map.end()) {
      return Status::InvalidArgs();
    }
    vec.push_back(it->second);
  }
  return Serialize(vec, buf_writer, schema);
}

size_t SimpleRow::GetTypeLength_(ValueType type) noexcept {
  switch (type) {
  case ValueType::Int32:
  case ValueType::Float:
    return 4;
  case ValueType::Int64:
  case ValueType::Double:
  case ValueType::String:
    return 8;
  case ValueType::Bool:
    return 1;
  default:
    break;
  }
  UNREACHABLE();
}

} // namespace property
} // namespace arcanedb