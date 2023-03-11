/**
 * @file versioned_delta_node.h
 * @author sheep (ysj1173886760@gmail.com)
 * @brief
 * @version 0.1
 * @date 2023-01-23
 *
 * @copyright Copyright (c) 2023
 *
 */
#pragma once

#include "btree/btree_type.h"
#include "common/options.h"
#include "property/row/row.h"
#include "property/sort_key/sort_key.h"
#include <atomic>
#include <cstddef>
#include <memory>
#include <string>

namespace arcanedb {
namespace btree {

class VersionedDeltaNode : public RowOwner {
  friend class VersionedBwTreePage;

  struct Entry {
    // highest bit stores whether row is deleted.
    // 31 bit stores offset
    uint32_t control_bit{};
    // TODO(sheep): 16 entry per cache line. false sharing might hurt
    // performance
    std::atomic<TxnTs> write_ts{};

    Entry() = default;
    Entry(const Entry &rhs) noexcept
        : control_bit(rhs.control_bit),
          write_ts(rhs.write_ts.load(std::memory_order_relaxed)) {}
  };

  static constexpr size_t kDefaultVersionChainLength = 8;

  using VersionContainer =
      std::vector<absl::InlinedVector<Entry, kDefaultVersionChainLength>>;

public:
  // ctor for insert and update
  VersionedDeltaNode(const property::Row &row, TxnTs ts) noexcept;

  // ctor for delete
  VersionedDeltaNode(property::SortKeysRef sort_key, TxnTs ts) noexcept;

  VersionedDeltaNode(std::string buffer, std::string version_buffer,
                     std::vector<Entry> rows,
                     VersionContainer versions) noexcept
      : buffer_(std::move(buffer)), version_buffer_(std::move(version_buffer)),
        rows_(std::move(rows)), versions_(std::move(versions)) {}

  void SetPrevious(std::shared_ptr<VersionedDeltaNode> previous) noexcept {
    total_length_ = (previous == nullptr ? 0 : previous->GetTotalLength()) + 1;
    previous_ = std::move(previous);
  }

  std::shared_ptr<VersionedDeltaNode> GetPrevious() const noexcept {
    return previous_;
  }

  size_t GetSize() const noexcept { return rows_.size(); }

  size_t GetTotalLength() const noexcept { return total_length_; }

  void SetLSN(log_store::LsnType lsn) noexcept {
    lsn_.store(lsn, std::memory_order_relaxed);
  }

  log_store::LsnType GetLSN() noexcept {
    return lsn_.load(std::memory_order_relaxed);
  }

  /**
   * @brief
   * Traverse the delta node.
   * @tparam Visitor
   * Visitor requires three parameter, first is const Row&,
   * second indicates whether row has been deleted.
   * third is write_ts of that row.
   * we will traverse new version first, then old version
   * !! do not store reference to the row, since it was a variable on stack.
   * @param visitor
   */
  template <typename Visitor>
  log_store::LsnType Traverse(Visitor visitor, bool should_lock = false) const
      noexcept {
    log_store::LsnType lsn;
    if (unlikely(should_lock)) {
      lock_.Lock();
    }

    // read lsn inside lock.
    lsn = lsn_.load();
    for (size_t i = 0; i < rows_.size(); i++) {
      {
        auto offset = GetOffset(rows_[i].control_bit);
        auto row = property::Row(buffer_.data() + offset);
        visitor(row, IsDeleted(rows_[i].control_bit),
                rows_[i].write_ts.load(std::memory_order_relaxed));
      }
      if (version_buffer_.empty()) {
        continue;
      }
      for (const Entry &entry : versions_[i]) {
        auto offset = GetOffset(entry.control_bit);
        auto row = property::Row(version_buffer_.data() + offset);
        visitor(row, IsDeleted(entry.control_bit),
                entry.write_ts.load(std::memory_order_relaxed));
      }
    }

    if (unlikely(should_lock)) {
      lock_.Unlock();
    }
    return lsn;
  }

  /**
   * @brief
   * Point read
   * @param sort_key
   * @param read_ts
   * @param opts
   * @param view
   * @return Status
   */
  Status GetRow(property::SortKeysRef sort_key, TxnTs read_ts,
                const Options &opts, RowView *view) const noexcept {
    // first locate sort_key
    auto it = std::lower_bound(
        rows_.begin(), rows_.end(), sort_key,
        [&](const Entry &entry, const property::SortKeysRef &sort_key) {
          auto offset = GetOffset(entry.control_bit);
          auto row = property::Row(buffer_.data() + offset);
          return row.GetSortKeys() < sort_key;
        });
    if (it == rows_.end()) {
      return Status::NotFound();
    }

    auto &entry = *it;
    auto offset = GetOffset(entry.control_bit);
    auto row = property::Row(buffer_.data() + offset);
    if (row.GetSortKeys() != sort_key) {
      // sk not match
      return Status::NotFound();
    }

    // only newest version can be locked
    // relaxed here is ok since we will acquire lock outside, which has the
    // acquire semantic.
    if (auto write_ts = entry.write_ts.load(std::memory_order_relaxed);
        IsLocked(write_ts)) {
      if (opts.ignore_lock) {
        // skip the intent, since we are read-only txn
      } else if (opts.owner_ts.has_value() &&
                 *opts.owner_ts == GetTs(write_ts)) {
        // ignore, since we are the owner.
      } else {
        return Status::RowLocked();
      }
    }

    // try read newest version
    if (IsVisible_(read_ts, entry.write_ts.load(std::memory_order_relaxed))) {
      return ReadVersion_(row, entry, view);
    }

    // no old version
    if (versions_.empty()) {
      return Status::NotFound();
    }

    // try old versions
    auto index = std::distance(rows_.begin(), it);
    for (const Entry &version : versions_[index]) {
      if (IsVisible_(read_ts, version.write_ts)) {
        auto offset = GetOffset(version.control_bit);
        auto row = property::Row(version_buffer_.data() + offset);
        return ReadVersion_(row, version, view);
      }
    }
    return Status::NotFound();
  }

  /**
   * @brief
   * Set ts of the newest version with "sort_key" to "target_ts"
   * @param sort_key
   * @param target_ts
   * @param lsn
   * @return Status
   */
  Status SetTs(property::SortKeysRef sort_key, TxnTs target_ts,
               log_store::LsnType lsn) noexcept {
    // first locate sort_key
    auto it = std::lower_bound(
        rows_.begin(), rows_.end(), sort_key,
        [&](const Entry &entry, const property::SortKeysRef &sort_key) {
          auto offset = GetOffset(entry.control_bit);
          auto row = property::Row(buffer_.data() + offset);
          return row.GetSortKeys() < sort_key;
        });
    if (it == rows_.end()) {
      return Status::NotFound();
    }

    auto &entry = *it;
    auto offset = GetOffset(entry.control_bit);
    auto row = property::Row(buffer_.data() + offset);
    if (row.GetSortKeys() != sort_key) {
      // sk not match
      return Status::NotFound();
    }

    // must be locked
    if (IsLocked(entry.write_ts.load(std::memory_order_relaxed))) {
      lock_.Lock();
      lsn_.store(lsn, std::memory_order_relaxed);
      entry.write_ts.store(target_ts, std::memory_order_relaxed);
      lock_.Unlock();
      return Status::Ok();
    }
    // first entry is not locked, logical error might happens
    UNREACHABLE();
  }

  VersionedDeltaNode() = default;

  std::string TEST_DumpChain() const noexcept;

private:
  friend class VersionedDeltaNodeBuilder;

  inline static bool IsVisible_(TxnTs read_ts, TxnTs write_ts) noexcept {
    // aborted version is not visible
    if (write_ts == kAbortedTxnTs) {
      return false;
    }
    return read_ts >= write_ts;
  }

  // hope to inline
  inline Status ReadVersion_(const property::Row &row, const Entry &entry,
                             RowView *view) const noexcept {
    if (IsDeleted(entry.control_bit)) {
      return Status::Deleted();
    }
    view->PushBackRef(
        RowWithTs(row, entry.write_ts.load(std::memory_order_relaxed)));
    view->AddOwnerPointer(shared_from_this());
    return Status::Ok();
  }

  static constexpr size_t kStateOffset = 31;
  static constexpr size_t kOffsetMask = 0x7fffffff;
  static constexpr size_t kMaximumOffset = kOffsetMask;

  static bool IsDeleted(uint32_t control_bit) noexcept {
    return (control_bit >> kStateOffset) & 1;
  }

  static size_t GetOffset(uint32_t control_bit) noexcept {
    return control_bit & kOffsetMask;
  }

  static void MarkDeleted(Entry *entry) noexcept {
    entry->control_bit = entry->control_bit | (1 << kStateOffset);
  }

  std::string buffer_{};
  std::string version_buffer_{};
  std::vector<Entry> rows_{};
  VersionContainer versions_;
  std::shared_ptr<VersionedDeltaNode> previous_{};
  uint32_t total_length_{};
  std::atomic<log_store::LsnType> lsn_{};
  // spin lock is used to protect the atomicity of
  // lsn and timestamp.
  mutable absl::base_internal::SpinLock lock_;
};

class VersionedDeltaNodeBuilder {
  friend class VersionedBwTreePage;

public:
  VersionedDeltaNodeBuilder() = default;

  void AddDeltaNode(const VersionedDeltaNode *node) noexcept;

  std::shared_ptr<VersionedDeltaNode> GenerateDeltaNode() noexcept;

  size_t GetRowSize() const noexcept { return map_.size(); }

  size_t GetDeltaCount() const noexcept { return delta_cnt_; }

private:
  struct BuildEntry {
    const property::Row row;
    bool is_deleted;
    TxnTs write_ts;
  };

  template <typename Container>
  static void WriteRow_(Container &container, util::BufWriter *writer,
                        const BuildEntry &build_entry) noexcept {
    VersionedDeltaNode::Entry entry;
    entry.control_bit = writer->Offset();
    entry.write_ts.store(build_entry.write_ts, std::memory_order_relaxed);
    if (build_entry.is_deleted) {
      VersionedDeltaNode::MarkDeleted(&entry);
    }
    writer->WriteBytes(build_entry.row.as_slice());
    container.emplace_back(entry);
  }

  std::map<property::SortKeysRef, std::vector<BuildEntry>> map_;
  size_t delta_cnt_;
};

} // namespace btree
} // namespace arcanedb