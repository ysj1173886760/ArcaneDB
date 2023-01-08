/**
 * @file bwtree_page.cpp
 * @author sheep (ysj1173886760@gmail.com)
 * @brief
 * @version 0.1
 * @date 2023-01-08
 *
 * @copyright Copyright (c) 2023
 *
 */

#include "btree/page/bwtree_page.h"
#include "butil/logging.h"

namespace arcanedb {
namespace btree {

Status InsertRow(const handler::LogicalTuple &tuple,
                 const property::Schema *schema) noexcept {
  NOTIMPLEMENTED();
}

Status UpdateRow(const handler::LogicalTuple &tuple,
                 const property::Schema *schema) noexcept {
  NOTIMPLEMENTED();
}

Status DeleteRow(const handler::LogicalTuple &tuple,
                 const property::Schema *schema) noexcept {
  NOTIMPLEMENTED();
}

Status GetRow(const handler::LogicalTuple &tuple,
              const property::Schema *schema) noexcept {
  NOTIMPLEMENTED();
}

} // namespace btree
} // namespace arcanedb