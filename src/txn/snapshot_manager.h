/**
 * @file snapshot_manager.h
 * @author sheep (ysj1173886760@gmail.com)
 * @brief
 * @version 0.1
 * @date 2023-01-24
 *
 * @copyright Copyright (c) 2023
 *
 */

#pragma once

#include "absl/hash/hash.h"
#include "common/config.h"
#include "common/status.h"
#include "common/type.h"
#include <set>

namespace arcanedb {
namespace txn {

/**
 * @brief
 * TxnManager is used to manage txn ts.
 * i.e. tract the snapshot ts for read-only txn.
 */
class SnapshotManager {
public:
  SnapshotManager() = default;

  void RegisterTs(TxnTs ts) noexcept {
    util::InstrumentedLockGuard<ArcanedbLock> guard(mu_);
    ts_set_.insert(ts);
  }

  void CommitTs(TxnTs ts) noexcept {
    util::InstrumentedLockGuard<ArcanedbLock> guard(mu_);
    ts_set_.erase(ts);
  }

  TxnTs GetSnapshotTs() const noexcept {
    util::InstrumentedLockGuard<ArcanedbLock> guard(mu_);
    return ts_set_.empty() ? kMaxTxnTs : (*ts_set_.begin() - 1);
  }

private:
  mutable ArcanedbLock mu_;
  std::set<TxnTs> ts_set_;
};

class ShardedSnapshotManager {
public:
  explicit ShardedSnapshotManager(size_t shard_num) : shards_(shard_num) {}

  static ShardedSnapshotManager *GetSnapshotManager() noexcept {
    static ShardedSnapshotManager snapshot_manager(
        common::Config::kSnapshotManagerShardNum);
    return &snapshot_manager;
  }

  void RegisterTs(TxnTs ts) noexcept { GetShard_(ts)->RegisterTs(ts); }

  void CommitTs(TxnTs ts) noexcept { GetShard_(ts)->CommitTs(ts); }

  TxnTs GetSnapshotTs() noexcept {
    TxnTs ts = kMaxTxnTs;
    for (const auto &shard : shards_) {
      ts = std::min(ts, shard.GetSnapshotTs());
    }
    return ts;
  }

private:
  SnapshotManager *GetShard_(TxnTs ts) noexcept {
    return &shards_[absl::Hash<TxnTs>()(ts) % shards_.size()];
  }

  std::vector<SnapshotManager> shards_;
};

} // namespace txn
} // namespace arcanedb