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
#include "leveldb/cache.h"
#include "leveldb/filter_policy.h"
#include "util/bthread_util.h"
#include "common/status.h"

namespace arcanedb {
namespace leveldb_store {

Status AsyncLevelDB::Open(const std::string &name,
                    std::shared_ptr<AsyncLevelDB> *async_leveldb,
                    std::shared_ptr<util::ThreadPool> thread_pool,
                    const Options &options) noexcept {
  *async_leveldb = std::make_shared<AsyncLevelDB>();
  (*async_leveldb)->thread_pool_ = thread_pool;
  leveldb::Options leveldb_options;
  leveldb_options.write_buffer_size = options.write_buffer_size;
  leveldb_options.block_cache = leveldb::NewLRUCache(options.block_cache_size);
  // 10 is leveldb recommend value
  leveldb_options.filter_policy = leveldb::NewBloomFilterPolicy(10);
  leveldb_options.create_if_missing = true;
  // async open
  return util::LaunchAsync([&]() {
    leveldb::DB *db;
    leveldb::Status status = leveldb::DB::Open(leveldb_options, name, &db);
    if (!status.ok()) {
      LOG_WARN("Failed to open db, name %s", name.c_str());
      return Status::Err("Failed to open level db");
    }
    (*async_leveldb)->db_ = db;
    return Status::Ok();
  }, thread_pool)->Get();
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