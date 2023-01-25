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
#include "property/row/row.h"
#include "property/sort_key/sort_key.h"
#include <memory>
#include <string>

namespace arcanedb {
namespace btree {

class VersionedDeltaNode : public RowOwner {
  struct Entry {
    // highest bit stores whether row is deleted.
    // 31 bit stores offset
    uint32_t control_bit;
    TxnTs write_ts;
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
    if (previous != nullptr) {
      total_length_ = previous->GetTotalLength() + 1;
    }
    previous_ = std::move(previous);
  }

  std::shared_ptr<VersionedDeltaNode> GetPrevious() const noexcept {
    return previous_;
  }

  size_t GetSize() const noexcept { return rows_.size(); }

  size_t GetTotalLength() const noexcept { return total_length_; }

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
  template <typename Visitor> void Traverse(Visitor visitor) const noexcept {
    for (size_t i = 0; i < rows_.size(); i++) {
      {
        auto offset = GetOffset(rows_[i]);
        auto row = property::Row(buffer_.data() + offset);
        visitor(row, IsDeleted(rows_[i]), rows_[i].write_ts);
      }
      if (version_buffer_.empty()) {
        continue;
      }
      for (Entry entry : versions_[i]) {
        auto offset = GetOffset(entry);
        auto row = property::Row(version_buffer_.data() + offset);
        visitor(row, IsDeleted(entry), entry.write_ts);
      }
    }
  }

  /**
   * @brief
   * Point read.
   * @param sort_key
   * @param view
   * @param read_ts
   * @return Status
   */
  Status GetRow(property::SortKeysRef sort_key, TxnTs read_ts,
                RowView *view) const noexcept {
    // first locate sort_key
    auto it = std::lower_bound(
        rows_.begin(), rows_.end(), sort_key,
        [&](const Entry &entry, const property::SortKeysRef &sort_key) {
          auto offset = GetOffset(entry);
          auto row = property::Row(buffer_.data() + offset);
          return row.GetSortKeys() < sort_key;
        });
    if (it == rows_.end()) {
      return Status::NotFound();
    }
    auto entry = *it;
    auto offset = GetOffset(entry);
    auto row = property::Row(buffer_.data() + offset);
    if (row.GetSortKeys() != sort_key) {
      // sk not match
      return Status::NotFound();
    }
    // try read newest version
    if (IsVisible_(read_ts, entry.write_ts)) {
      return ReadVersion_(row, entry, view);
    }
    // no old version
    if (versions_.empty()) {
      return Status::NotFound();
    }
    // try old versions
    auto index = std::distance(rows_.begin(), it);
    // copy is fine here, since Entry only has 8 byte
    static_assert(sizeof(Entry) == 8, "use reference instead of copying value");
    for (Entry version : versions_[index]) {
      if (IsVisible_(read_ts, version.write_ts)) {
        auto offset = GetOffset(version);
        auto row = property::Row(version_buffer_.data() + offset);
        return ReadVersion_(row, version, view);
      }
    }
    return Status::NotFound();
  }

  VersionedDeltaNode() = default;

  std::string TEST_DumpChain() const noexcept;

private:
  friend class VersionedDeltaNodeBuilder;

  static bool IsVisible_(TxnTs read_ts, TxnTs write_ts) noexcept {
    return read_ts >= write_ts;
  }

  Status ReadVersion_(const property::Row &row, Entry entry,
                      RowView *view) const noexcept {
    if (IsDeleted(entry)) {
      return Status::Deleted();
    }
    view->PushBackRef(RowWithTs(row, entry.write_ts));
    view->AddOwnerPointer(shared_from_this());
    return Status::Ok();
  }

  static constexpr size_t kStateOffset = 31;
  static constexpr size_t kOffsetMask = 0x7fffffff;
  static constexpr size_t kMaximumOffset = kOffsetMask;

  static bool IsDeleted(Entry entry) noexcept {
    return (entry.control_bit >> kStateOffset) & 1;
  }

  static size_t GetOffset(Entry entry) noexcept {
    return entry.control_bit & kOffsetMask;
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
};

class VersionedDeltaNodeBuilder {
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
    entry.write_ts = build_entry.write_ts;
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