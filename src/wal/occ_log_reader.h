/**
 * @file occ_log_reader.h
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
#include "util/codec/buf_reader.h"
#include <string>

namespace arcanedb {
namespace wal {

struct BeginLog {
  TxnId txn_id;
  TxnTs begin_ts;
};

inline BeginLog DeserializeBeginLog(const std::string_view &data) noexcept {
  util::BufReader reader(data);
  BeginLog log;
  reader.ReadBytes(&log.txn_id);
  reader.ReadBytes(&log.begin_ts);
  return log;
};

struct CommitLog {
  TxnId txn_id;
  TxnTs commit_ts;
};

inline CommitLog DeserializeCommitLog(const std::string_view &data) noexcept {
  util::BufReader reader(data);
  CommitLog log;
  reader.ReadBytes(&log.txn_id);
  reader.ReadBytes(&log.commit_ts);
  return log;
};

struct AbortLog {
  TxnId txn_id;
};

inline AbortLog DeserializeAbortLog(const std::string_view &data) noexcept {
  util::BufReader reader(data);
  AbortLog log;
  reader.ReadBytes(&log.txn_id);
  return log;
};

} // namespace wal
} // namespace arcanedb