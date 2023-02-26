/**
 * @file occ_recovery.h
 * @author sheep (ysj1173886760@gmail.com)
 * @brief
 * @version 0.1
 * @date 2023-02-26
 *
 * @copyright Copyright (c) 2023
 *
 */

#pragma once

#include "cache/buffer_pool.h"
#include "log_store/log_store.h"

namespace arcanedb {
namespace txn {

class OccRecovery {
public:
  OccRecovery(cache::BufferPool *buffer_pool,
              log_store::LogReader *log_reader) noexcept
      : buffer_pool_(buffer_pool), log_reader_(log_reader) {}

  void Recover() noexcept;

private:
  void BwTreeSetRow_(const std::string_view &data) noexcept;
  void BwTreeDeleteRow_(const std::string_view &data) noexcept;
  void BwTreeSetTs_(const std::string_view &data) noexcept;
  void OccBegin_(const std::string_view &data) noexcept;
  void OccAbort_(const std::string_view &data) noexcept;
  void OccCommit_(const std::string_view &data) noexcept;

  void AddPrepare_(TxnId txn_id) noexcept;
  void AddCommit_(TxnId txn_id) noexcept;

  cache::BufferPool *buffer_pool_{};
  log_store::LogReader *log_reader_{};

  std::unordered_map<TxnId, uint32_t> txn_map_;
};

} // namespace txn
} // namespace arcanedb