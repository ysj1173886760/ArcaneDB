/**
 * @file write_info.h
 * @author sheep (ysj1173886760@gmail.com)
 * @brief
 * @version 0.1
 * @date 2023-01-28
 *
 * @copyright Copyright (c) 2023
 *
 */

#pragma once

#include "log_store/log_store.h"

namespace arcanedb {
namespace btree {

/**
 * @brief
 * Write info contains the result of a writing to btree.
 * e.g. LSN, page size.
 */
struct WriteInfo {
  log_store::LsnType lsn{log_store::kInvalidLsn};
  bool is_dirty{false};
};

} // namespace btree
} // namespace arcanedb