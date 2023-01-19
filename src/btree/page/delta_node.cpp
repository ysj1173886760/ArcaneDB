/**
 * @file delta_node.cpp
 * @author sheep (ysj1173886760@gmail.com)
 * @brief
 * @version 0.1
 * @date 2023-01-18
 *
 * @copyright Copyright (c) 2023
 *
 */

#include "btree/page/delta_node.h"
#include "butil/logging.h"
#include <optional>

namespace arcanedb {
namespace btree {

DeltaNode::DeltaNode(const property::Row &row) noexcept {
  // copy the row
  auto slice = row.as_slice();
  util::BufWriter writer;
  writer.WriteBytes(slice);
  buffer_ = writer.Detach();
  // offset is zero
  Entry entry = 0;
  rows_.push_back(entry);
}

DeltaNode::DeltaNode(property::SortKeysRef sort_key) noexcept {
  // serialize row
  util::BufWriter writer;
  property::Row::SerializeOnlySortKey(sort_key, &writer);
  buffer_ = writer.Detach();
  // offset is zero
  Entry entry = 0;
  rows_.push_back(MarkDeleted(entry));
}

void DeltaNodeBuilder::AddDeltaNode(const DeltaNode &node) noexcept {
  node.Traverse([&](RowRef ref, bool is_deleted) {
    auto it = map_.find(ref.get().GetSortKeys());
    if (it != map_.end()) {
      return;
    }
    map_.emplace(ref.get().GetSortKeys(),
                 BuildEntry{.ref = ref, .is_deleted = is_deleted});
  });
}

std::shared_ptr<DeltaNode> DeltaNodeBuilder::GenerateDeltaNode() noexcept {
  // serialize nodes
  util::BufWriter writer;
  std::vector<DeltaNode::Entry> rows;
  int current_offset = 0;
  for (const auto &[sk, build_entry] : map_) {
    CHECK(current_offset < DeltaNode::kMaximumOffset);
    DeltaNode::Entry entry = current_offset;
    if (build_entry.is_deleted) {
      entry = DeltaNode::MarkDeleted(entry);
    }
    writer.WriteBytes(build_entry.ref.get().as_slice());
    rows.push_back(entry);
    current_offset = writer.Offset();
  }
  auto node = std::make_shared<DeltaNode>();
  node->buffer_ = writer.Detach();
  node->rows_ = std::move(rows);
  return node;
}

} // namespace btree
} // namespace arcanedb