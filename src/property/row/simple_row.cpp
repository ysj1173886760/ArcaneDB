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
#include "butil/logging.h"
#include "common/macros.h"
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
  NOTIMPLEMENTED();
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