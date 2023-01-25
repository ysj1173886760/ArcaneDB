/**
 * @file lock_table.h
 * @author sheep (ysj1173886760@gmail.com)
 * @brief
 * @version 0.1
 * @date 2023-01-24
 *
 * @copyright Copyright (c) 2023
 *
 */

#pragma once

#include "absl/container/flat_hash_map.h"
#include "bthread/condition_variable.h"
#include "bthread/mutex.h"
#include "common/config.h"
#include "common/status.h"
#include "common/type.h"
#include "property/sort_key/sort_key.h"
#include "txn/txn_type.h"
#include <limits>

namespace arcanedb {
namespace txn {

class LockTable {
public:
  LockTable() = default;

  // TODO(sheep): implement RwLock.

  /**
   * @brief
   *
   * @param sort_key
   * @param txn_id
   * @return Status
   */
  Status Lock(std::string_view sort_key, TxnId txn_id) noexcept;

  /**
   * @brief
   *
   * @param sort_key
   * @param txn_id
   * @return Status
   */
  Status Unlock(std::string_view sort_key, TxnId txn_id) noexcept;

private:
  struct LockEntry {
    TxnId txn_id{};
    bthread::ConditionVariable cv{};
    uint8_t ref_cnt{0};
    bool is_locked{true};

    LockEntry(TxnId id) : txn_id(id) {}

    void AddWaiter() noexcept {
      CHECK(ref_cnt < std::numeric_limits<uint8_t>::max());
      ref_cnt += 1;
    }

    void DecWaiter() noexcept { ref_cnt -= 1; }

    bool ShouldGC() const noexcept { return ref_cnt == 0; }
  };

  using ContainerType =
      absl::flat_hash_map<std::string, std::unique_ptr<LockEntry>>;

  ContainerType map_; // guarded by mu_;
  // TODO(sheep): monitor the wait latency
  bthread::Mutex mu_;
};

class ShardedLockTable {
public:
  explicit ShardedLockTable(size_t shard_num) noexcept : shards_(shard_num) {}

  Status Lock(std::string_view sort_key, TxnId txn_id) noexcept {
    return GetShard_(sort_key)->Lock(sort_key, txn_id);
  }

  Status Unlock(std::string_view sort_key, TxnId txn_id) noexcept {
    return GetShard_(sort_key)->Unlock(sort_key, txn_id);
  }

private:
  LockTable *GetShard_(std::string_view sort_key) noexcept {
    return &shards_[absl::Hash<std::string_view>()(sort_key) % shards_.size()];
  }

  std::vector<LockTable> shards_;
};

} // namespace txn
} // namespace arcanedb