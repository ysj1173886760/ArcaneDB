/**
 * @file simple_row.h
 * @author sheep (ysj1173886760@gmail.com)
 * @brief
 * @version 0.1
 * @date 2023-01-08
 *
 * @copyright Copyright (c) 2023
 *
 */

#pragma once

#include "property/row/row_concept.h"

namespace arcanedb {
namespace property {

/**
 * @brief
 * Native row implementation
 * Format:
 * | Column1 | Column2 | ... | ColumnN | String1 | String2 |
 * For string type, it will occupy 8bytes, first 4 byte is pointer,
 * second 4 byte is length
 */
class SimpleRow : RowConcept<SimpleRow> {
public:
  /**
   * @brief Get property by column id
   *
   * @param id column id
   * @param[out] value property
   * @param schema
   * @return Status
   */
  Status GetProp(ColumnId id, Value *value, Schema *schema) noexcept;

  /**
   * @brief
   * Serialize a row.
   * @param value_ref_vec vector that stores value reference.
   * following constraint should be satisfied:
   *  value_ref_vec.size() == schema->size() &&
   * value_ref_vec[i].type == schema.column[i].type
   * @param buf_writer
   * @param schema
   * @return Status
   */
  Status Serialize(const ValueRefVec &value_ref_vec,
                   util::BufWriter *buf_writer, Schema *schema) noexcept;

private:
  static size_t GetTypeLength_(ValueType type) noexcept;

  const char *ptr_{nullptr};
};

} // namespace property
} // namespace arcanedb