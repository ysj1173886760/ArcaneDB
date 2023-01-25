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
#include <optional>
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
    max_ts_ = std::max(max_ts_, ts);
  }

  void CommitTs(TxnTs ts) noexcept {
    util::InstrumentedLockGuard<ArcanedbLock> guard(mu_);
    ts_set_.erase(ts);
    // ARCANEDB_INFO("commit ts {}", ts);
  }

  std::pair<bool, TxnTs> GetSnapshotTs() const noexcept {
    util::InstrumentedLockGuard<ArcanedbLock> guard(mu_);
    // if there is no concurrent txn,
    // we will use the max ts we have ever seen.
    if (ts_set_.empty()) {
      return {false, max_ts_};
    }
    return {true, *ts_set_.begin() - 1};
  }

  std::string TEST_DumpState() const noexcept {
    auto [has_concurrent, ts] = GetSnapshotTs();
    return fmt::format("{} {}", has_concurrent, ts);
  }

private:
  mutable ArcanedbLock mu_{"SnapshotManager"};
  std::set<TxnTs> ts_set_;
  TxnTs max_ts_{};
};

class ShardedSnapshotManager {
public:
  explicit ShardedSnapshotManager(size_t shard_num) : shards_(shard_num) {}

  void RegisterTs(TxnTs ts) noexcept { GetShard_(ts)->RegisterTs(ts); }

  void CommitTs(TxnTs ts) noexcept { GetShard_(ts)->CommitTs(ts); }

  TxnTs GetSnapshotTs() const noexcept {
    // TODO(sheep): cache result
    TxnTs min_ts = kMaxTxnTs;
    bool has_concurrent = false;
    TxnTs max_ts = 0;
    for (const auto &shard : shards_) {
      auto [has_concurrent_txn, ts] = shard.GetSnapshotTs();
      if (has_concurrent_txn) {
        has_concurrent = true;
        min_ts = std::min(min_ts, ts);
      } else {
        max_ts = std::max(ts, max_ts);
      }
    }
    return has_concurrent ? min_ts : max_ts;
  }

  void TEST_PrintSnapshotTs() noexcept {
    for (const auto &shard : shards_) {
      ARCANEDB_INFO("{} ", shard.GetSnapshotTs().second);
    }
  }

  std::string TEST_DumpState() noexcept {
    std::string result = "dump state: ";
    for (const auto &shard : shards_) {
      result += shard.TEST_DumpState() + ", ";
    }
    return result;
  }

private:
  SnapshotManager *GetShard_(TxnTs ts) noexcept {
    return &shards_[absl::Hash<TxnTs>()(ts) % shards_.size()];
  }

  std::vector<SnapshotManager> shards_;
};

} // namespace txn
} // namespace arcanedb