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

namespace arcanedb {
namespace property {

Status SimpleRow::GetProp(ColumnId id, Value *value, Schema *schema) noexcept {
  DCHECK(ptr_ != nullptr);
  NOTIMPLEMENTED();
}

Status SimpleRow::Serialize(const ValueRefVec &value_ref_vec,
                            util::BufWriter *buf_writer,
                            Schema *schema) noexcept {
  NOTIMPLEMENTED();
}

} // namespace property
} // namespace arcanedb