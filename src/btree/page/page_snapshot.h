#pragma once

#include "log_store/log_store.h"

namespace arcanedb {
namespace btree {

class PageSnapshot {
public:
  virtual std::string Serialize() noexcept = 0;

  virtual log_store::LsnType GetLSN() noexcept = 0;

  virtual ~PageSnapshot() noexcept {}
};

} // namespace btree
} // namespace arcanedb