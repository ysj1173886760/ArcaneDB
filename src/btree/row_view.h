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

#include "property/row/row.h"
#include <functional>
#include <vector>

namespace arcanedb {
namespace btree {

using RowRef = std::reference_wrapper<const property::Row>;

using RowsView = std::vector<RowRef>;

} // namespace btree
} // namespace arcanedb