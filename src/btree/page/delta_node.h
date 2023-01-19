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

#include "btree/row_view.h"
#include "property/row/row.h"
#include "property/sort_key/sort_key.h"
#include <string>

namespace arcanedb {
namespace btree {

class DeltaNode;

class DeltaNodeBuilder {
public:
  DeltaNodeBuilder() = default;

  void AddDeltaNode(const DeltaNode &node) noexcept;

  std::shared_ptr<DeltaNode> GenerateDeltaNode() noexcept;

private:
  struct BuildEntry {
    RowRef ref;
    bool is_deleted;
  };
  std::map<property::SortKeysRef, BuildEntry> map_;
};

class DeltaNode {
public:
  // ctor for insert and update
  explicit DeltaNode(const property::Row &row) noexcept;

  // ctor for delete
  explicit DeltaNode(property::SortKeysRef sort_key) noexcept;

  void SetPrevious(std::shared_ptr<DeltaNode> previous) noexcept {
    previous_ = std::move(previous);
  }

  std::shared_ptr<DeltaNode> GetPrevious() noexcept { return previous_; }

  /**
   * @brief
   * Traverse the delta node.
   * @tparam Visitor
   * Visitor requires two parameter, first is RowRef,
   * second indicates whether row has been deleted.
   * @param visitor
   */
  template <typename Visitor> void Traverse(Visitor visitor) const noexcept {
    for (const auto &entry : rows_) {
      auto offset = GetOffset(entry);
      auto row = property::Row(buffer_.data() + offset);
      visitor(row, IsDeleted(entry));
    }
  }

  DeltaNode() = default;

private:
  friend class DeltaNodeBuilder;

  enum class DeltaState {
    kUpdate,
    kDelete,
  };

  using Entry = uint16_t;
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
};

} // namespace btree
} // namespace arcanedb