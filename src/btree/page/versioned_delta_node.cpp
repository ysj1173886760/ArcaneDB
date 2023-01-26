/**
 * @file versioned_delta_node.cpp
 * @author sheep (ysj1173886760@gmail.com)
 * @brief
 * @version 0.1
 * @date 2023-01-23
 *
 * @copyright Copyright (c) 2023
 *
 */

#include "btree/page/versioned_delta_node.h"

namespace arcanedb {
namespace btree {

VersionedDeltaNode::VersionedDeltaNode(const property::Row &row,
                                       TxnTs ts) noexcept {
  // copy the row
  auto slice = row.as_slice();
  util::BufWriter writer;
  writer.WriteBytes(slice);
  buffer_ = writer.Detach();
  Entry entry;
  // offset is zero
  entry.control_bit = 0;
  // assign ts
  entry.write_ts = ts;
  rows_.push_back(entry);
}

VersionedDeltaNode::VersionedDeltaNode(property::SortKeysRef sort_key,
                                       TxnTs ts) noexcept {
  // serialize row
  util::BufWriter writer;
  property::Row::SerializeOnlySortKey(sort_key, &writer);
  buffer_ = writer.Detach();
  Entry entry;
  // offset is zero
  entry.control_bit = 0;
  MarkDeleted(&entry);
  // assign ts
  entry.write_ts = ts;
  rows_.push_back(entry);
}

void VersionedDeltaNodeBuilder::AddDeltaNode(
    const VersionedDeltaNode *node) noexcept {
  node->Traverse([&](const property::Row &row, bool is_deleted,
                     TxnTs write_ts) {
    // skip aborted version
    if (write_ts == kAbortedTxnTs) {
      return;
    }
    // TODO(sheep): remove old version
    map_[row.GetSortKeys()].emplace_back(
        BuildEntry{.row = row, .is_deleted = is_deleted, .write_ts = write_ts});
  });
  delta_cnt_ += 1;
}

std::shared_ptr<VersionedDeltaNode>
VersionedDeltaNodeBuilder::GenerateDeltaNode() noexcept {
  // generate rows_, buffer_, versions_, version_buffer_
  util::BufWriter writer;
  util::BufWriter version_writer;
  std::vector<VersionedDeltaNode::Entry> rows;
  VersionedDeltaNode::VersionContainer versions;
  bool has_version = false;
  for (const auto &[sk, vec] : map_) {
    // process newest version
    WriteRow_(rows, &writer, vec[0]);
    // process old version
    // push empty
    versions.push_back({});
    for (int i = 1; i < vec.size(); i++) {
      WriteRow_(versions.back(), &version_writer, vec[i]);
    }
    has_version = has_version || vec.size() > 1;
  }
  if (!has_version) {
    versions.clear();
  }
  return std::make_shared<VersionedDeltaNode>(
      writer.Detach(), version_writer.Detach(), std::move(rows),
      std::move(versions));
}

std::string VersionedDeltaNode::TEST_DumpChain() const noexcept {
  struct BuildEntry {
    const property::Row row;
    bool is_deleted;
    TxnTs write_ts;
  };
  std::map<property::SortKeysRef, std::vector<BuildEntry>> map;
  auto current_ptr = this;
  // traverse the delta node
  while (current_ptr != nullptr) {
    current_ptr->Traverse(
        [&](const property::Row &row, bool is_deleted, TxnTs write_ts) {
          map[row.GetSortKeys()].emplace_back(BuildEntry{
              .row = row, .is_deleted = is_deleted, .write_ts = write_ts});
        });
    current_ptr = current_ptr->GetPrevious().get();
  }
  std::string result = "DeltaChainDump:\n";
  for (const auto &[sk, vec] : map) {
    result += sk.ToString() + ", ";
    for (const auto &entry : vec) {
      result += fmt::format("{} {}, ", entry.write_ts, entry.is_deleted);
    }
    result += "\n";
  }
  return result;
}

} // namespace btree
} // namespace arcanedb