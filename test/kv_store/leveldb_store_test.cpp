/**
 * @file leveldb_store_test.cpp
 * @author sheep (ysj1173886760@gmail.com)
 * @brief
 * @version 0.1
 * @date 2022-12-11
 *
 * @copyright Copyright (c) 2022
 *
 */

#include "kv_store/leveldb_store.h"
#include "util/thread_pool.h"
#include <gtest/gtest.h>
#include <memory>
#include <string>

namespace arcanedb {
namespace leveldb_store {

TEST(LeveldbStoreTest, BasicTest) {
  auto thread_pool = std::make_shared<util::ThreadPool>(10);
  const std::string db_name = "./testdb";
  std::shared_ptr<AsyncLevelDB> db;
  auto s = AsyncLevelDB::Open(db_name, &db, thread_pool, Options());
  EXPECT_TRUE(s.ok());

  // put 10 key
  for (int i = 0; i < 10; i++) {
    EXPECT_TRUE(db->Put(std::to_string(i), std::to_string(i + 1)).ok());
  }
  // read 10 key
  for (int i = 0; i < 10; i++) {
    std::string value;
    EXPECT_TRUE(db->Get(std::to_string(i), &value).ok());
    EXPECT_EQ(value, std::to_string(i + 1));
  }
  // delete 10 key
  for (int i = 0; i < 10; i++) {
    std::string value;
    EXPECT_TRUE(db->Delete(std::to_string(i)).ok());
  }
  for (int i = 0; i < 10; i++) {
    std::string value;
    EXPECT_TRUE(db->Get(std::to_string(i), &value).IsNotFound());
  }
  db.reset();
  EXPECT_TRUE(AsyncLevelDB::DestroyDB(db_name).ok());
}

} // namespace leveldb_store
} // namespace arcanedb