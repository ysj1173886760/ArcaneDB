#pragma once

#include "property/row/row_concept.h"
#include "property/sort_key/sort_key.h"
#include "util/codec/buf_writer.h"
namespace arcanedb {
namespace property {

constexpr size_t kRowTotalLengthSize = 2;
constexpr size_t kRowSortKeyLengthSize = 2;
constexpr size_t kRowSortKeyLengthOffset = 2;
constexpr size_t kRowSortKeyOffset = 4;

// TODO(sheep): Support HasSortKey and HasValue
/**
 * @brief
 * RowFormat:
 * | Total length 2 byte | SortKey Length 2 byte | SortKey varlen | Columns... |
 */
class Row : public RowConcept<Row> {
public:
  Row(const char *ptr) noexcept : ptr_(ptr) {}

  Row() = default;

  /**
   * @brief Get property by column id
   *
   * @param id column id
   * @param[out] value property
   * @param schema
   * @return Status
   */
  Status GetProp(ColumnId id, ValueResult *value, Schema *schema) noexcept;

  SortKeysRef GetSortKeys() noexcept;

  /**
   * @brief
   * Serialize a row.
   * @param value_ref_vec vector that stores value reference.
   * following constraint should be satisfied:
   * value_ref_vec.size() == schema->size() &&
   * value_ref_vec[i].type == schema.column[i].type
   * @param buf_writer
   * @param schema
   * @return Status
   */
  static Status Serialize(const ValueRefVec &value_ref_vec,
                          util::BufWriter *buf_writer, Schema *schema) noexcept;

  void SerializeOnlySortKey(SortKeysRef sort_key,
                            util::BufWriter *buf_writer) noexcept;

private:
  Status GetPropNormalValue_(size_t index, ValueResult *value,
                             Schema *schema) noexcept;

  Status GetPropSortKey_(size_t index, ValueResult *value,
                         Schema *schema) noexcept;

  static size_t GetTypeLength_(ValueType type) noexcept;

  const char *ptr_{nullptr};
};

} // namespace property
} // namespace arcanedb