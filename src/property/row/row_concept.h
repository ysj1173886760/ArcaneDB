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
#include "property/sort_key/sort_key.h"
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
  Status GetProp(ColumnId id, ValueResult *value,
                 const Schema *schema) const noexcept {
    return Real()->GetProp(id, value, schema);
  }

  // TODO(sheep): impl GetProps

  SortKeysRef GetSortKeys() const noexcept { return Real()->GetSortKeys(); }

private:
  RowType *Real() noexcept { return static_cast<RowType *>(this); }
};

} // namespace property
} // namespace arcanedb