/**
 * @file leveldb_store.h
 * @author sheep (ysj1173886760@gmail.com)
 * @brief simple kv store that wrapped leveldb with async interface.
 * @version 0.1
 * @date 2022-12-11
 *
 * @copyright Copyright (c) 2022
 *
 */

#pragma once

#include "common/status.h"

namespace arcanedb {
namespace leveldb_store {

class AsyncLevelDB {
public:
  static Status Open(const std::string &name,
                     std::shared_ptr<AsyncLevelDB> *async_leveldb) noexcept;

  Status Put(const std::string_view &key,
             const std::string_view &value) noexcept;

  Status Delete(const std::string_view &key) noexcept;

  Status Get(const std::string_view &key, std::string *value) noexcept;
};

} // namespace leveldb_store
} // namespace arcanedb