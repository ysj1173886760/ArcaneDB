/**
 * @file row_concept.h
 * @author sheep (ysj1173886760@gmail.com)
 * @brief
 * @version 0.1
 * @date 2023-01-07
 *
 * @copyright Copyright (c) 2023
 *
 */
#pragma once

#include "common/status.h"
#include "property/property_type.h"
#include "property/schema.h"
#include "util/codec/buf_writer.h"

namespace arcanedb {
namespace property {

template <typename RowType> class RowConcept {
public:
  /**
   * @brief Get property by column id
   *
   * @param id column id
   * @param[out] value property
   * @param schema
   * @return Status
   */
  Status GetProp(ColumnId id, Value *value, Schema *schema) noexcept {
    return Real()->GetProp(id, value);
  }

  /**
   * @brief
   * Serialize a row.
   * @param value_ref_map mapping from column id to value reference
   * @param[out] buf_writer result buffer
   * @param schema
   * @return Status
   */
  Status Serialize(const ValueRefMap &value_ref_map,
                   util::BufWriter *buf_writer, Schema *schema) noexcept {
    return Real()->Serialize(value_ref_map, buf_writer, schema);
  }

private:
  RowType *Real() noexcept { return static_cast<RowType *>(this); }
};

} // namespace property
} // namespace arcanedb