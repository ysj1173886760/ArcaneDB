/**
 * @file txn_manager.h
 * @author sheep (ysj1173886760@gmail.com)
 * @brief
 * @version 0.1
 * @date 2023-01-27
 *
 * @copyright Copyright (c) 2023
 *
 */

#pragma once

#include "common/options.h"
#include <memory>

namespace arcanedb {
namespace txn {

class TxnContext;

class TxnManager {
public:
  virtual std::unique_ptr<TxnContext>
  BeginRoTxn(const Options &opts) const noexcept = 0;

  virtual std::unique_ptr<TxnContext>
  BeginRwTxn(const Options &opts) const noexcept = 0;

  virtual ~TxnManager() noexcept {}
};

} // namespace txn
} // namespace arcanedb