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

namespace arcanedb {
namespace btree {

DeltaNode::DeltaNode(const property::Row &row) noexcept {
  util::BufWriter writer;
  auto slice = row.as_slice();
  writer.WriteBytes(slice);
  buffer_ = writer.Detach();
  state_ = DeltaState::kUpdate;
}

DeltaNode::DeltaNode(property::SortKeysRef sort_key) noexcept {
  util::BufWriter writer;
  property::Row::SerializeOnlySortKey(sort_key, &writer);
  buffer_ = writer.Detach();
  state_ = DeltaState::kDelete;
}

} // namespace btree
} // namespace arcanedb