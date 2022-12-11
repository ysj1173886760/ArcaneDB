/**
 * @file leveldb_store.cc
 * @author sheep (ysj1173886760@gmail.com)
 * @brief 
 * @version 0.1
 * @date 2022-12-11
 * 
 * @copyright Copyright (c) 2022
 * 
 */

#include "kv_store/leveldb_store.h"

namespace arcanedb {
namespace leveldb_store {

Status AsyncLevelDB::Open(const std::string &name, std::shared_ptr<AsyncLevelDB> *async_leveldb) noexcept {
  return Status::Ok();
}

Status AsyncLevelDB::Put(const std::string_view &key, const std::string_view &value) noexcept {
  return Status::Ok();
}

Status AsyncLevelDB::Delete(const std::string_view &key) noexcept {
  return Status::Ok();
}

Status AsyncLevelDB::Get(const std::string_view& key, std::string *value) noexcept {
  return Status::Ok();
}

}
}