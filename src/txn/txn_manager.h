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

#include <memory>

namespace arcanedb {
namespace txn {

class TxnContext;

class TxnManager {
public:
  virtual std::unique_ptr<TxnContext> BeginRoTxn() const noexcept = 0;

  virtual std::unique_ptr<TxnContext> BeginRwTxn() const noexcept = 0;

  virtual ~TxnManager() noexcept {}
};

}
}