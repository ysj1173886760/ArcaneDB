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
  return util::LaunchAsync(
             [&]() {
               leveldb::DB *db;
               leveldb::Status status =
                   leveldb::DB::Open(leveldb_options, name, &db);
               if (!status.ok()) {
                 ARCANEDB_WARN("Failed to open db, name %s, error: %s",
                               name.c_str(), status.ToString().c_str());
                 return Status::Err();
               }
               (*async_leveldb)->db_ = db;
               (*async_leveldb)->cache_ = leveldb_options.block_cache;
               (*async_leveldb)->filter_policy_ = leveldb_options.filter_policy;
               return Status::Ok();
             },
             thread_pool)
      ->Get();
}

Status AsyncLevelDB::Put(const std::string_view &key,
                         const std::string_view &value) noexcept {
  return util::LaunchAsync(
             [&]() {
               leveldb::WriteOptions options;
               leveldb::Status status =
                   db_->Put(options, leveldb::Slice(key.data(), key.size()),
                            leveldb::Slice(value.data(), value.size()));
               if (!status.ok()) {
                 ARCANEDB_WARN("Failed to put, key {}, error: {}", key,
                               status.ToString());
                 return Status::Err();
               }
               return Status::Ok();
             },
             thread_pool_)
      ->Get();
}

Status AsyncLevelDB::Delete(const std::string_view &key) noexcept {
  return util::LaunchAsync(
             [&]() {
               leveldb::WriteOptions options;
               leveldb::Status status =
                   db_->Delete(options, leveldb::Slice(key.data(), key.size()));
               if (!status.ok()) {
                 ARCANEDB_WARN("Failed to delete, key {}, error: {}", key,
                               status.ToString());
                 return Status::Err();
               }
               return Status::Ok();
             },
             thread_pool_)
      ->Get();
}

Status AsyncLevelDB::Get(const std::string_view &key,
                         std::string *value) noexcept {
  // TODO: consider using snapshot for reading
  return util::LaunchAsync(
             [&]() {
               leveldb::ReadOptions options;
               leveldb::Status status = db_->Get(
                   options, leveldb::Slice(key.data(), key.size()), value);
               if (status.IsNotFound()) {
                 return Status::NotFound();
               }
               if (!status.ok()) {
                 ARCANEDB_WARN("Failed to get, key {}, error: {}", key,
                               status.ToString());
                 return Status::Err();
               }
               return Status::Ok();
             },
             thread_pool_)
      ->Get();
}

Status AsyncLevelDB::DestroyDB(const std::string &name) noexcept {
  auto status = leveldb::DestroyDB(name, leveldb::Options());
  if (!status.ok()) {
    ARCANEDB_WARN("Failed to destory db {}, error: {}", name,
                  status.ToString());
    return Status::Err("Failed to destroy");
  }
  return Status::Ok();
}

AsyncLevelDB::~AsyncLevelDB() {
  delete db_;
  delete cache_;
  delete filter_policy_;
}

} // namespace leveldb_store
} // namespace arcanedb