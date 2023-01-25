/**
 * @file txn_context.cpp
 * @author sheep (ysj1173886760@gmail.com)
 * @brief
 * @version 0.1
 * @date 2023-01-25
 *
 * @copyright Copyright (c) 2023
 *
 */

#include "txn/txn_context.h"

namespace arcanedb {
namespace txn {

Status TxnContext::SetRow(const std::string &sub_table_key,
                          const property::Row &row,
                          const Options &opts) noexcept {}

Status TxnContext::DeleteRow(const std::string &sub_table_key,
                             property::SortKeysRef sort_key,
                             const Options &opts) noexcept {}

Status TxnContext::GetRow(const std::string &sub_table_key,
                          property::SortKeysRef sort_key, const Options &opts,
                          btree::RowView *view) noexcept {}

void TxnContext::Commit() noexcept {}

} // namespace txn
} // namespace arcanedb
