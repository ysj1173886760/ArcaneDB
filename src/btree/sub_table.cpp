/**
 * @file sub_table.cpp
 * @author sheep (ysj1173886760@gmail.com)
 * @brief
 * @version 0.1
 * @date 2023-01-25
 *
 * @copyright Copyright (c) 2023
 *
 */

#include "btree/sub_table.h"
#include "cache/buffer_pool.h"
#include "common/logger.h"

namespace arcanedb {
namespace btree {

Status SubTable::OpenSubTable(const std::string &table_key, const Options &opts,
                              std::unique_ptr<SubTable> *sub_table) noexcept {
  // root page id is table key
  VersionedBtreePage *root_page;
  auto s = opts.buffer_pool->GetPage<VersionedBtreePage>(table_key, &root_page);
  if (!s.ok()) {
    return s;
  }
  *sub_table = std::make_unique<SubTable>(root_page);
  return Status::Ok();
}

} // namespace btree
} // namespace arcanedb