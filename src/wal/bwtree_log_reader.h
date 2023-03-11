/**
 * @file bwtree_log_reader.h
 * @author sheep (ysj1173886760@gmail.com)
 * @brief
 * @version 0.1
 * @date 2023-02-26
 *
 * @copyright Copyright (c) 2023
 *
 */

#pragma once

#include "common/type.h"
#include "property/row/row.h"
#include "wal/log_type.h"
#include <string>

namespace arcanedb {
namespace wal {

namespace detail {
inline std::string_view
DeserializeString(util::BufReader *buf_reader) noexcept {
  uint16_t length;
  CHECK(buf_reader->ReadBytes(&length));
  std::string_view str;
  CHECK(buf_reader->ReadPiece(&str, length));
  return str;
}
} // namespace detail

struct SetRowLog {
  std::string_view page_id;
  TxnId txn_id;
  TxnTs write_ts;
  property::Row row;
};

inline SetRowLog DeserializeSetRowLog(const std::string_view &data) noexcept {
  util::BufReader reader(data);
  SetRowLog log;
  reader.ReadBytes(&log.txn_id);
  log.page_id = detail::DeserializeString(&reader);
  reader.ReadBytes(&log.write_ts);
  log.row = property::Row(reader.CurrentPtr());
  return log;
}

struct DeleteRowLog {
  std::string_view page_id;
  TxnId txn_id;
  TxnTs write_ts;
  property::SortKeysRef sort_key;
};

inline DeleteRowLog
DeserializeDeleteRowLog(const std::string_view &data) noexcept {
  util::BufReader reader(data);
  DeleteRowLog log;
  reader.ReadBytes(&log.txn_id);
  log.page_id = detail::DeserializeString(&reader);
  reader.ReadBytes(&log.write_ts);
  log.sort_key = property::SortKeysRef(detail::DeserializeString(&reader));
  return log;
}

struct SetTsLog {
  std::string_view page_id;
  TxnId txn_id;
  TxnTs commit_ts;
  property::SortKeysRef sort_key;
};

inline SetTsLog DeserializeSetTsLog(const std::string_view &data) noexcept {
  util::BufReader reader(data);
  SetTsLog log;
  reader.ReadBytes(&log.txn_id);
  log.page_id = detail::DeserializeString(&reader);
  reader.ReadBytes(&log.commit_ts);
  log.sort_key = property::SortKeysRef(detail::DeserializeString(&reader));
  return log;
}

} // namespace wal
} // namespace arcanedb