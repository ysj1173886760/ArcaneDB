/**
 * @file options.h
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
#include <optional>

namespace arcanedb {
namespace cache {
class BufferPool;
}
namespace log_store {
class LogStore;
}
namespace property {
class Schema;
}

/**
 * @brief
 * Options for reading and writing btree.
 */
struct Options {
  const property::Schema *schema{};
  cache::BufferPool *buffer_pool{};
  log_store::LogStore *log_store{};
  // we will skip the lock when lock ts is the same as
  // owner ts.
  std::optional<TxnTs> owner_ts{};
  TxnId txn_id{};
  bool disable_compaction{false};
  bool ignore_lock{false};
  bool force_compaction{false};
  bool check_intent_locked{false};
  bool sync_commit{false};
};

} // namespace arcanedb