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
#include "leveldb/db.h"
#include "util/thread_pool.h"

namespace arcanedb {
namespace leveldb_store {

struct Options {
  // memtable size
  size_t write_buffer_size = 4 << 20;
  // cache size
  size_t block_cache_size = 8 << 20;
};

class AsyncLevelDB {
public:
  static Status Open(const std::string &name,
                     std::shared_ptr<AsyncLevelDB> *async_leveldb,
                     std::shared_ptr<util::ThreadPool> thread_pool,
                     const Options &options) noexcept;

  ~AsyncLevelDB();

  Status Put(const std::string_view &key,
             const std::string_view &value) noexcept;

  Status Delete(const std::string_view &key) noexcept;

  Status Get(const std::string_view &key, std::string *value) noexcept;

  static Status DestroyDB(const std::string &name) noexcept;

private:
  leveldb::DB *db_;
  std::shared_ptr<util::ThreadPool> thread_pool_;
};

} // namespace leveldb_store
} // namespace arcanedb