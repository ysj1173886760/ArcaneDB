#include "property/row/row.h"
#include "common/macros.h"
#include "property/property_type.h"
#include "property/sort_key/comparable_buf_reader.h"
#include "property/sort_key/sort_key.h"
#include "util/codec/buf_writer.h"
#include "util/codec/encoding.h"

namespace arcanedb {
namespace property {

Status Row::GetProp(ColumnId id, ValueResult *value,
                    const Schema *schema) const noexcept {
  auto index = schema->GetColumnIndex(id);
  if (index < schema->GetSortKeyCount()) {
    return GetPropSortKey_(index, value, schema);
  }
  return GetPropNormalValue_(index, value, schema);
}

SortKeysRef Row::GetSortKeys() const noexcept {
  auto length = util::DecodeFixed16(ptr_ + kRowSortKeyLengthOffset);
  return SortKeysRef(std::string_view(ptr_ + kRowSortKeyOffset, length));
}

Status Row::Serialize(const ValueRefVec &value_ref_vec,
                      util::BufWriter *buf_writer,
                      const Schema *schema) noexcept {
  CHECK(value_ref_vec.size() == schema->GetColumnNum());
  // first generate sort keys
  auto sort_key_cnt = schema->GetSortKeyCount();
  // TODO(sheep): one copy could be eliminated
  auto sort_key = SortKeys(value_ref_vec, sort_key_cnt);
  auto header_pos = buf_writer->Offset();

  // reserve header
  buf_writer->Reserve(kRowTotalLengthSize);

  // serialize sort key
  buf_writer->WriteBytes(static_cast<uint16_t>(sort_key.as_slice().size()));
  buf_writer->WriteBytes(sort_key.as_slice());

  // serialize columns
  auto column_cnt = schema->GetColumnNum();
  absl::InlinedVector<std::string_view, kDefaultColumnNum> va_fields;
  size_t string_offset = kRowTotalLengthSize + kRowSortKeyLengthSize +
                         sort_key.as_slice().size() +
                         schema->GetColumnOffsetForRow(column_cnt);
  uint16_t total_length =
      kRowTotalLengthSize + kRowSortKeyLengthSize + sort_key.as_slice().size();

  for (size_t i = sort_key_cnt; i < column_cnt; i++) {
    auto type = schema->GetColumnRefByIndex(i)->type;
    CHECK(static_cast<uint8_t>(type) == value_ref_vec[i].index());
    switch (type) {
    case ValueType::Int32:
    case ValueType::Float: {
      total_length += 4;
      buf_writer->WriteVariant(value_ref_vec[i]);
      break;
    }
    case ValueType::Int64:
    case ValueType::Double: {
      total_length += 8;
      buf_writer->WriteVariant(value_ref_vec[i]);
      break;
    }
    case ValueType::Bool: {
      total_length += 1;
      buf_writer->WriteVariant(value_ref_vec[i]);
      break;
    }
    case ValueType::String: {
      auto data = std::get<std::string_view>(value_ref_vec[i]);
      uint16_t offset = string_offset;
      uint16_t length = data.size();
      va_fields.push_back(data);
      buf_writer->WriteBytes(offset);
      buf_writer->WriteBytes(length);
      string_offset += length;
      total_length += 4 + length;
      break;
    }
    default:
      UNREACHABLE();
    }
  }
  // serialize string
  for (const auto &str : va_fields) {
    buf_writer->WriteBytes(str);
  }
  // write total length
  buf_writer->WriteBytesAtPos(header_pos, total_length);
  return Status::Ok();
}

void Row::SerializeOnlySortKey(SortKeysRef sort_key,
                               util::BufWriter *buf_writer) noexcept {
  uint16_t total_length =
      kRowTotalLengthSize + kRowSortKeyLengthSize + sort_key.as_slice().size();
  buf_writer->WriteBytes(total_length);
  buf_writer->WriteBytes(static_cast<uint16_t>(sort_key.as_slice().size()));
  buf_writer->WriteBytes(sort_key.as_slice());
}

Status Row::GetPropNormalValue_(size_t index, ValueResult *value,
                                const Schema *schema) const noexcept {
  DCHECK(ptr_ != nullptr);
  auto sort_key_length = util::DecodeFixed16(ptr_ + kRowSortKeyLengthOffset);
  auto offset = schema->GetColumnOffsetForRow(index) + sort_key_length +
                kRowSortKeyLengthSize + kRowSortKeyLengthSize;
  auto type = schema->GetColumnRefByIndex(index)->type;
  std::string_view value_ref(ptr_ + offset, GetTypeLength_(type));
  switch (type) {
  case ValueType::Int32: {
    int32_t v;
    util::ReadBuf(value_ref.data(), &v);
    value->value = v;
    break;
  }
  case ValueType::Int64: {
    int64_t v;
    util::ReadBuf(value_ref.data(), &v);
    value->value = v;
    break;
  }
  case ValueType::Float: {
    float v;
    util::ReadBuf(value_ref.data(), &v);
    value->value = v;
    break;
  }
  case ValueType::Double: {
    double v;
    util::ReadBuf(value_ref.data(), &v);
    value->value = v;
    break;
  }
  case ValueType::Bool: {
    uint8_t v;
    util::ReadBuf(value_ref.data(), &v);
    value->value = (v != 0);
    break;
  }
  case ValueType::String: {
    uint16_t string_offset;
    uint16_t string_length;
    util::ReadBuf(value_ref.data(), &string_offset);
    util::ReadBuf(value_ref.data() + 2, &string_length);
    value->value = std::string_view(ptr_ + string_offset, string_length);
    break;
  }
  default:
    UNREACHABLE();
  }
  return Status::Ok();
}

Status Row::GetPropSortKey_(size_t index, ValueResult *value,
                            const Schema *schema) const noexcept {
  auto sort_key_length = util::DecodeFixed16(ptr_ + kRowSortKeyLengthOffset);
  ComparableBufReader reader(
      std::string_view(ptr_ + kRowSortKeyOffset, sort_key_length));
  reader.SkipK(index);
  OwnedValue v;
  reader.ReadValue(&v);
  auto type = schema->GetColumnRefByIndex(index)->type;
  CHECK(static_cast<ValueType>(v.index()) == type);
  switch (type) {
  case ValueType::Bool: {
    value->value = std::get<bool>(v);
    break;
  }
  case ValueType::Int32: {
    value->value = std::get<int32_t>(v);
    break;
  }
  case ValueType::Float: {
    value->value = std::get<float>(v);
    break;
  }
  case ValueType::Int64: {
    value->value = std::get<int64_t>(v);
    break;
  }
  case ValueType::Double: {
    value->value = std::get<double>(v);
    break;
  }
  case ValueType::String: {
    value->owned_str =
        std::make_unique<std::string>(std::get<std::string>(std::move(v)));
    value->value = *value->owned_str;
    break;
  }
  default:
    UNREACHABLE();
    break;
  }
  return Status::Ok();
}

size_t Row::GetTypeLength_(ValueType type) noexcept {
  switch (type) {
  case ValueType::Int32:
  case ValueType::Float:
  case ValueType::String:
    return 4;
  case ValueType::Int64:
  case ValueType::Double:
    return 8;
  case ValueType::Bool:
    return 1;
  default:
    break;
  }
  UNREACHABLE();
}

std::string_view Row::as_slice() const noexcept {
  assert(ptr_);
  auto total_length = util::DecodeFixed16(ptr_);
  return std::string_view(ptr_, total_length);
}

} // namespace property
} // namespace arcanedb