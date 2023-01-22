/**
 * @file row_view.h
 * @author sheep (ysj1173886760@gmail.com)
 * @brief
 * @version 0.1
 * @date 2023-01-19
 *
 * @copyright Copyright (c) 2023
 *
 */

#pragma once

#include "common/type.h"
#include "property/row/row.h"
#include "util/view.h"
#include <functional>
#include <memory>
#include <vector>

namespace arcanedb {
namespace btree {

/**
 * @brief
 * Owner base
 */
class RowOwner : public std::enable_shared_from_this<RowOwner> {
public:
  virtual ~RowOwner() noexcept {}
};

using RowView = util::Views<property::Row, RowOwner>;

} // namespace btree
} // namespace arcanedb