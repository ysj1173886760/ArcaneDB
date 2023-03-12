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

Status SubTable::OpenSubTable(const std::string_view &table_key,
                              const Options &opts,
                              std::unique_ptr<SubTable> *sub_table) noexcept {
  // root page id is table key
  cache::BufferPool::PageHolder page_holder;
  auto s = opts.buffer_pool->GetPage(table_key, &page_holder);
  if (!s.ok()) {
    return s;
  }
  *sub_table = std::make_unique<SubTable>(page_holder);
  return Status::Ok();
}

} // namespace btree
} // namespace arcanedb