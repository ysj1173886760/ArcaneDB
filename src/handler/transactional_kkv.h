/**
 * @file transactional_kkv.h
 * @author sheep (ysj1173886760@gmail.com)
 * @brief 
 * @version 0.1
 * @date 2023-01-27
 * 
 * @copyright Copyright (c) 2023
 * 
 */

#pragma once

#include "txn/txn_context.h"
#include "cache/buffer_pool.h"
#include "common/status.h"
#include "txn/txn_manager.h"

// !! deprecated

namespace arcanedb {
namespace handler {

class TransactionalKKV {
public:
  static Status Open(std::unique_ptr<TransactionalKKV> *db, const Options &opts) noexcept;

  std::unique_ptr<txn::TxnContext> BeginRoTxn() noexcept;

  std::unique_ptr<txn::TxnContext> BeginRwTxn() noexcept;

private:
  std::unique_ptr<txn::TxnManager> txn_manager_;
  std::unique_ptr<cache::BufferPool> buffer_pool_;
};

}
}