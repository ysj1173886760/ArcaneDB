/**
 * @file transactional_kkv.cpp
 * @author sheep (ysj1173886760@gmail.com)
 * @brief
 * @version 0.1
 * @date 2023-01-27
 *
 * @copyright Copyright (c) 2023
 *
 */

#include "handler/transactional_kkv.h"
#include "txn/txn_manager_occ.h"

namespace arcanedb {
namespace handler {

Status TransactionalKKV::Open(std::unique_ptr<TransactionalKKV> *db,
                              const Options &opts) noexcept {
  auto res = std::make_unique<TransactionalKKV>();
  res->txn_manager_ =
      std::make_unique<txn::TxnManagerOCC>(txn::LockManagerType::kCentralized);
  res->buffer_pool_ = std::make_unique<cache::BufferPool>();
  *db = std::move(res);
  return Status::Ok();
}

std::unique_ptr<txn::TxnContext>
TransactionalKKV::BeginRoTxn(const Options &opts) noexcept {
  return txn_manager_->BeginRoTxn(opts);
}

std::unique_ptr<txn::TxnContext>
TransactionalKKV::BeginRwTxn(const Options &opts) noexcept {
  return txn_manager_->BeginRwTxn(opts);
}

} // namespace handler
} // namespace arcanedb