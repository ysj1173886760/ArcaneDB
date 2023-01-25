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

namespace arcanedb {
namespace btree {

Status SubTable::OpenSubTable(const std::string_view &table_key,
                              const Options &opts,
                              std::unique_ptr<SubTable> *sub_table) noexcept {
  NOTIMPLEMENTED();
}

} // namespace btree
} // namespace arcanedb