/**
 * @file delta_node.h
 * @author sheep (ysj1173886760@gmail.com)
 * @brief
 * @version 0.1
 * @date 2023-01-18
 *
 * @copyright Copyright (c) 2023
 *
 */

#include "btree/btree_type.h"
#include "property/row/row.h"
#include "property/sort_key/sort_key.h"
#include <memory>
#include <string>

namespace arcanedb {
namespace btree {

class DeltaNode;

class DeltaNodeBuilder {
public:
  DeltaNodeBuilder() = default;

  void AddDeltaNode(const DeltaNode *node) noexcept;

  std::shared_ptr<DeltaNode> GenerateDeltaNode() noexcept;

  size_t GetRowSize() const noexcept { return map_.size(); }

  size_t GetDeltaCount() const noexcept { return delta_cnt_; }

private:
  struct BuildEntry {
    const property::Row row;
    bool is_deleted;
  };
  std::map<property::SortKeysRef, BuildEntry> map_;
  size_t delta_cnt_;
};

class DeltaNode : public RowOwner {
  using Entry = uint16_t;

public:
  // ctor for insert and update
  explicit DeltaNode(const property::Row &row) noexcept;

  // ctor for delete
  explicit DeltaNode(property::SortKeysRef sort_key) noexcept;

  // ctor for delta builder
  DeltaNode(std::string buffer, std::vector<Entry> rows) noexcept
      : rows_(std::move(rows)), buffer_(std::move(buffer)) {}

  void SetPrevious(std::shared_ptr<DeltaNode> previous) noexcept {
    if (previous != nullptr) {
      total_length_ = previous->GetTotalLength() + 1;
    }
    previous_ = std::move(previous);
  }

  std::shared_ptr<DeltaNode> GetPrevious() noexcept { return previous_; }

  size_t GetSize() const noexcept { return rows_.size(); }

  size_t GetTotalLength() const noexcept { return total_length_; }

  /**
   * @brief
   * Traverse the delta node.
   * @tparam Visitor
   * Visitor requires two parameter, first is const Row&,
   * second indicates whether row has been deleted.
   * !! do not store reference to the row, since it was a variable on stack.
   * @param visitor
   */
  template <typename Visitor> void Traverse(Visitor visitor) const noexcept {
    for (const auto &entry : rows_) {
      auto offset = GetOffset(entry);
      auto row = property::Row(buffer_.data() + offset);
      visitor(row, IsDeleted(entry));
    }
  }

  /**
   * @brief
   * Point read
   * @param sort_key
   * @param view
   * @return Status
   */
  Status GetRow(property::SortKeysRef sort_key, RowView *view) const noexcept {
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
      return Status::NotFound();
    }
    if (IsDeleted(entry)) {
      return Status::Deleted();
    }
    view->PushBackRef(row);
    view->AddOwnerPointer(shared_from_this());
    return Status::Ok();
  }

  DeltaNode() = default;

private:
  friend class DeltaNodeBuilder;

  enum class DeltaState {
    kUpdate,
    kDelete,
  };

  static constexpr size_t kStateOffset = 15;
  static constexpr size_t kOffsetMask = 0x7fff;
  static constexpr size_t kMaximumOffset = kOffsetMask;

  static bool IsDeleted(Entry entry) noexcept {
    return (entry >> kStateOffset) & 1;
  }

  static size_t GetOffset(Entry entry) noexcept { return entry & kOffsetMask; }

  static Entry MarkDeleted(Entry entry) noexcept {
    return entry | (1 << kStateOffset);
  }

  // highest bit stores delta state.
  std::vector<Entry> rows_{};
  // TODO(sheep): optimize memory allocation
  std::string buffer_{};
  std::shared_ptr<DeltaNode> previous_{};
  uint32_t total_length_{};
};

} // namespace btree
} // namespace arcanedb