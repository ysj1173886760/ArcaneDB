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

#include "property/row/row.h"
#include <string>

namespace arcanedb {
namespace btree {

class DeltaNode {
public:
  explicit DeltaNode(const property::Row &row, bool is_delete) noexcept;

private:
  enum class DeltaState {
    kUpdate,
    kDelete,
  };

  // TODO(support multiple row)
  property::Row row_;
  DeltaState state_;
  std::string buffer_;
  std::shared_ptr<DeltaNode> previous_;
};

} // namespace btree
} // namespace arcanedb